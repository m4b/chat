(* Compile with:
ocamlbuild.native -tag thread -lib unix src/chat.native
*)

let version = "v1.0.2"
let kBUFFER_SIZE = 4096

let debug = ref false

(* *********************** *)
(* Printers *)
(* *********************** *)

let pp_saddr ppf saddr =
  match saddr with
  | Unix.ADDR_INET (addr,port) ->
    Format.fprintf ppf "@[%s:%d@?@]" (Unix.string_of_inet_addr addr) port
  | _ -> Format.fprintf ppf ""

let show_saddr saddr =
  pp_saddr Format.str_formatter saddr; Format.flush_str_formatter()

let print_saddr saddr =
  pp_saddr Format.std_formatter saddr


(* *********************** *)
(* Helper Functions *)
(* *********************** *)

(* performs hostname resolution; localhost, m4b.xyz, etc. *)
let resolve_hostname host port =
  match Unix.getaddrinfo host (string_of_int port)
          (* we force tcp and ipv4 for simplicity *)
          [Unix.AI_SOCKTYPE (Unix.SOCK_STREAM); Unix.AI_FAMILY (Unix.PF_INET)]
  with
  | [] -> failwith @@ Printf.sprintf "<chat> Could not resolve host: %s:%d" host port
  | result ->
    try
      (* we'll pick the first one *)
      let info =
        List.find (fun info ->
            info.Unix.ai_socktype = Unix.SOCK_STREAM
        ) result
      in
      Format.printf "@[resolved %s to %s@.@]"
        host
        (show_saddr info.Unix.ai_addr);
      info.Unix.ai_addr |> function Unix.ADDR_INET (host, port) -> host | _ ->
        failwith @@ Printf.sprintf "<chat> Could find a suitable resolving host: %s:%d" host port
    with Not_found ->
      failwith @@ Printf.sprintf "<chat> Could find a suitable resolving host: %s:%d" host port

(* *********************** *)
(* Message Data Structure *)
(* *********************** *)

(* a message has a kind, which is identified by a single byte at the beginning of a received or sent byte sequence
  Correct messages are either:
    1. a sent message (\000 prefixed), followed by 8 bytes specifying the msg content size
    2. an acc of a sent message (\001 prefixed), followed by 8 bytes specifizing the msg content size
  Incorrect messages are either bad (too small) or unknown, likely coming from a connection which does not implement this "protocol"
 *)

(* NOTE on content size byte sequence.  
   1. In a production system we'd use a Uleb128 for maximum space efficiency (variable width unsigned integer).
   2. We're using OCaml's `int` (instead of int64) which actually uses the top bit for tagging; hence:
     0x7fff_ffff_ffff_ffff is actually the largest representable int, which is also signed.
     In practice, this is not very important, since it means our maximum message size representable 
     is approximately 0x7fff_ffff_ffff_ffff/2/8 bytes ~ 500 petabytes, which is enough for our humble chat program.
 *)

type msg_kind = | SEND of int | ACC of int | BAD | Unknown of int

let show_msg_kind kind =
  match kind with
  | SEND length -> Printf.sprintf "SEND[%d]" length
  | ACC length -> Printf.sprintf "ACC[%d]" length
  | BAD -> "BAD"
  | Unknown c -> Printf.sprintf "UNKNOWN 0x%x" c

(* A message thus consists of a kind (the kind byte prefix)
   and the contents (the remainder of the bytes, whatever they may be) *)
type msg = {
    kind: msg_kind;
    contents: bytes;
  }

let pp_msg ppf msg =
  Format.fprintf ppf "@[%s: %s@]" (show_msg_kind msg.kind) msg.contents

let show_msg msg =
  pp_msg Format.str_formatter msg; Format.flush_str_formatter()

let print_msg msg =
  pp_msg Format.std_formatter msg

(* I stole these from rdr *)

let set_uint bytes integer size offset = 
  for i = 0 to (size - 1) do
    let byte = Char.chr @@ ((integer lsr (8 * i)) land 0xff) in
    Bytes.set bytes (i + offset) byte
  done;
  offset + size

let u64o binary offset = 
  let res = ref (Char.code @@ Bytes.get binary offset) in
  for i = 1 to 7 do
        res := !res lor (Char.code (Bytes.get binary (i + offset)) lsl (i * 8)); (* ugh-life *)
  done;
  !res,(8+offset)

(* constructs a simple message with header/kind + contents from a byte sequence *)
let msg_of_bytes bytes len =
  if (len < 10) then
    {kind=BAD; contents=bytes}
  else
    let i64,offset = u64o bytes 1 in
    let kind =
      let k = Bytes.get bytes 0 |> Char.code in
      if (k = 0) then
        SEND i64
      else if (k = 1) then
        ACC i64
      else Unknown k
    in
    let contents = Bytes.sub bytes offset (len - offset) in
    {kind; contents}

(* hand-crafted json, if real we'd use jsonm or another json lib, 
  in addition to avoiding ^ string concat, and also appending the acc byte sequence more appropriately *)
let create_acc msg time =
  let json = Printf.sprintf "{acc: \"%s\", time: \"%f\"}" msg time in
  let length = Bytes.length json in
  let header = Printf.sprintf "\001ffffffff" in
  ignore @@ set_uint header length 8 1;
  header ^ json

(* similar to above, there is a _much_ better way of doing this
   (i.e., it's best to keep memory allocations as low as possible when sending),
   but suffices for simple demo *)
let create_send msg =
  let length = Bytes.length msg in
  let b = Bytes.make (length + 9) '\000' in
  Bytes.blit msg 0 b 9 length;
  ignore @@ set_uint b length 8 1;
  b

(* *********************** *)
(* Message Patching *)
(* *********************** *)

(* the type of message patch we're attempting to finish *)
type patch_kind = | ACC | SEND

(* Describes the state and information for patching a sequence of messages together *)
type patch = {
  kind: patch_kind;
  mutable current: int;
  total: int;
  buffer: Buffer.t;
}

let create_patch kind ~current ~total ~msg =
  let buffer = Buffer.create (kBUFFER_SIZE * 2) in
  Buffer.add_bytes buffer msg;
  {kind; current; total; buffer;}

let empty_patch =
    {kind = SEND; current = 0; total = 0; buffer = Buffer.create 0;}

(* patches the current segment of a broken up message with the prior contents,
   returns whether the patch is done, i.e. current >= total *)
let patch_msg msg ~sd ~count ~patch =
  patch.current <- patch.current + count;
  if (!debug) then Format.printf "@[<v 4>@ PATCH %d/%d@.@]" patch.current patch.total;
  Buffer.add_bytes patch.buffer (Bytes.sub msg 0 count);
  patch.current >= patch.total

(* *********************** *)
(* Thread Logic *)
(* *********************** *)

(* consumes a message of arbitrary size, and 
   returns the count to the caller along 
   with either a newly allocated byte sequence
   for large messages (>= kBUFFER_SIZE) 
   _or_ the pre-allocated small byte buffer sent in as a param
   with its contents filled up *)
let consume_msg sd buffer =
  let size = Unix.recv sd buffer 0 kBUFFER_SIZE [] in
  if (!debug) then Format.printf "@[<v 4>@ CONSUME: %d@.@?@]" size;
  if (size = kBUFFER_SIZE) then
    begin
      if (!debug) then Format.printf "@[<v 4>@ LARGE MSG@.@?@]";
      let total = ref kBUFFER_SIZE in
      let receiving = ref true in
      let tmp = Buffer.create (kBUFFER_SIZE*2) in
      Buffer.add_bytes tmp buffer;
      while !receiving do
        (* we need to poll in case
           the Bytes.length msg = kBUFFER_SIZE
           otherwise we block on waiting,
           which is what the initial call should do
         *)
        let read_l,_,_ = Unix.select [sd] [] [] 0.02 in
        if (read_l <> []) then
          begin
            let read = Unix.recv sd buffer 0 kBUFFER_SIZE [] in
            if (!debug) then Format.printf "@[<v 4>@ READ: %d@.@?@]" size;
            Buffer.add_bytes tmp buffer;
            if (read < kBUFFER_SIZE) then receiving := false;
            total := !total + read;
          end
        else receiving := false;
      done;
      (Buffer.to_bytes tmp),!total
    end
  else
    buffer,size

(* sends a message in totality *)
let send_msg sd msg =
  let total = Bytes.length msg in
  let count = Unix.send sd msg 0 (Bytes.length msg) [] in
  if (!debug) then
    Format.printf
      "@[<v 4>@ SEND %d/%d@.@?@]" count total;
  if (count < total) then
    let current = ref count in
    while !current < total do
      let pos,len = !current,((Bytes.length msg - !current)) in
      let count = Unix.send sd msg pos len [] in
      current := !current + count;
      if (!debug) then
        Format.printf
          "@[<v 4>@ SEND %d/%d@.@?@]" !current total
    done

(* prints the acc wed received; checks sent message in the stack against acc value for debuggin *)
let print_acc mutex accs msg =
  Mutex.lock mutex;
  let sentmsg = Stack.pop accs in
  Mutex.unlock mutex;
  if (!debug) then
    begin
      (* super hacky, again need proper json lib *)
      try
        let size = min (Bytes.length sentmsg) ((Bytes.length msg) - 7) in
        let extract = Bytes.sub_string msg 7 size in
        Format.printf "@[<v 4>@ ACC ROUNDTRIP PRESERVATION: %b@.@]"
          (extract = sentmsg)
      with _ -> Format.eprintf "@[<v 4>@ <WARN>: could not extract acc from msg %s@.@]" msg
    end;
  Format.printf "@[<v 2>@ << %s@.@]@[> @]@?" msg

let send_acc sd current msg =
  let time = Unix.gettimeofday () in
  current := time -. !current;
  Format.printf "@[@.< %s@.@]@[> @]@?" msg;
  let acc = create_acc msg time in
  send_msg sd acc

(* this thread has two responsibilities:
   1. receives bytes and prints them to the console
   2. sends accs on message receipt
 *)

let recv_fn sd saddr mutex running accs =
  let id = Thread.self() |> Thread.id in
  if (!debug) then Format.printf "@[<v 4>@ Recv thread (%d) %s@.@?@]" id (show_saddr saddr);
  let current_time = ref (Unix.gettimeofday ()) in
  let buffer = Bytes.create kBUFFER_SIZE in
  let patch = ref empty_patch in
  let patching_message = ref false in
  let prompt = ">" in
  try
    while !running do
      let ready = Thread.wait_timed_read sd 0.01 in
      if (ready) then
        begin
          let buffer,count = consume_msg sd buffer in
          if (!debug) then Format.printf "@[<v 4>@ RECV count: %d@.@?@]" count;
          if (count <= 0) then
            running := false
          else
            if (!patching_message) then
              begin
                if (!debug) then Format.printf "@[<v 4>@ PATCHING MESSAGE@.@?@]";
                if (patch_msg buffer ~sd:sd ~count:count ~patch:!patch) then
                  let msg = Bytes.sub (Buffer.to_bytes !patch.buffer) 0 !patch.total in
                  match !patch.kind with
                  | SEND ->
                     begin
                       if (!debug) then Format.printf "@[<v 4>@ PATCH SEND done %d/%d@.@]" !patch.current !patch.total;
                       patching_message := false;
                       send_acc sd current_time msg
                     end
                  | ACC ->
                     begin
                       if (!debug) then Format.printf "@[<v 4>@ PATCH ACC done %d/%d@.@]" !patch.current !patch.total;
                       patching_message := false;
                       print_acc mutex accs msg
                     end
              end
            else
              begin
                let msg = msg_of_bytes buffer count in
                if (!debug) then
                  begin
                    Format.printf "@[<v 4>@ MSG is %a@.@]" pp_msg msg;
                    Format.printf "@[<v 4>@ COUNT contents %d@.@]" (Bytes.length msg.contents);
                  end;
                match msg.kind with
                | ACC length ->
                   if (count < length) then
                     begin
                       patching_message := true;
                       patch := create_patch ACC ~current:count ~total:length ~msg:msg.contents
                     end
                   else
                     print_acc mutex accs msg.contents
                | SEND length ->
                   if (count < length) then
                     begin
                       patching_message := true;
                       patch := create_patch SEND ~current:count ~total:length ~msg:msg.contents
                     end
                   else
                     send_acc sd current_time msg.contents
                | BAD ->
                   Format.eprintf "@[@.<WARN> Received bad message: %a@.@[%s @]@?@]" pp_msg msg prompt
                | Unknown _ ->
                   Format.eprintf "@[@.<WARN> Received unknown message kind: %a@.@[%s @]@?@]" pp_msg msg prompt
              end
        end;
    done;
    running := false;
    if (!debug) then Format.printf "@[<v 4>@ Thread exiting: %d @.@]" id;
    Thread.exit();
  with
  | exn ->
     begin
       running := false;
       Format.eprintf "@[<v 4>@ RECV exc: %s@.@]" (Printexc.to_string exn);
       Thread.exit();
       raise exn
     end

(* 
   This send thread waits for user input, and then sends it.
   Currently it polls stdin every 100 milliseconds to see if it should return.
   This allows the server to restart listening 
   and join this thread properly instead of waiting for a final user input,
   as well as rate-limits the cpu usage on thread scheduling
 *)
let send_fn sd saddr mutex running accs =
  let id = Thread.self() |> Thread.id in
  let prompt = ">" in
  if (!debug) then Format.printf "@[<v 4>@ Send thread (%d) %s@.@?@]" id (show_saddr saddr);
  Format.printf "@[%s @]@?" prompt;
  try
    while !running do
      (* this lets us flip-flop from recv to send *)
      (*       Format.printf "@[<v 4>@ THREAD SEND@.@]"; *)
      let ready = Thread.wait_timed_read (Unix.descr_of_in_channel stdin) 0.01 in
      if (ready) then
        begin
          Format.printf "@[%s @]@?" prompt;
          try
            let msg = read_line () in
            if (Bytes.length msg > 0) then
              begin
                Mutex.lock mutex;
                Stack.push msg accs;
                Mutex.unlock mutex;
                let msg = create_send msg in
                send_msg sd msg
              end;
          with End_of_file ->
            begin
              Format.printf
                "@[<v 4>@ -=Goodbye!=-@.@]";
              running := false;
            end
        end
    done;
    running := false;
    if (!debug) then Format.printf "@[<v 4>@ Thread exiting: %d @.@]" id;
    Thread.exit();
  with
  | exn ->
    begin
      running := false;
      Format.eprintf "@[<v 4>@ SEND exc: %s@.@]" (Printexc.to_string exn);
      Thread.exit();
      raise exn
    end

(* the chat logic service; configured with a socket descriptor and socket address
   responsible for joining the recv and send threads, as well as closing the open fd/sd;
   currently the sent message stack is just for debugging, 
   checking if the sent/received/sent again bytes
   had roundtrip preservation
*)
let chat_logic sd saddr =
    let acc_stack = Stack.create () in
    let running = ref true in
    let mutex = Mutex.create () in
    Format.printf "@[<v 4>@ -=Welcome To Chat %s=-@.@?@]" version;
    let t1 = Thread.create (recv_fn sd saddr mutex running) acc_stack in
    let t2 = Thread.create (send_fn sd saddr mutex running) acc_stack in
    while !running do Thread.delay(0.2) done;
    if (!debug) then Format.printf "@[<v 4>@ CHAT joining thread: %d@.@]" (Thread.id t1);
    Thread.join t1;
    if (!debug) then Format.printf "@[<v 4>@ CHAT joining thread: %d@.@]" (Thread.id t2);
    Thread.join t2;
    if (!debug) then Format.printf "@[<v 4>@ CHAT closing fd@.@]";
    Unix.close sd

(* *********************** *)
(* Configuration *)
(* *********************** *)

let host = ref "127.0.0.1"
let port = ref 8888
let is_server = ref true

let set_host string =
  host := string

let get_host_and_port () =
  let host =
    try
      Unix.inet_addr_of_string !host
    with _ ->
      (* the name was bad, which means either it was actually bad or needs DNS resolution *)
      resolve_hostname !host !port
  in
  host,!port

let set_mode string =
  if (string = "client") then
    is_server := false

let usage = "Usage: chat [-h <host>] [-p <port>]"

let args =
  [
    ("-h", Arg.String set_host, "The host to connect to/listen on: default 127.0.0.1");
    ("-p", Arg.Set_int port, "The port to listen on: default 8888");
    ("-m", Arg.Symbol (["client"; "server"], set_mode), " mode: default server");
    ("-d", Arg.Set debug, "Set debug mode: default false");
  ]

(* *********************** *)
(* Main *)
(* *********************** *)

(* According to the command line arguments and configuration, either starts as a client or server;
   the server runs on an infinite loop, 
   and can be disconnected from and connected back to without restarting it
*)
let main () =
  let sd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let host,port = get_host_and_port () in
  let saddr = Unix.ADDR_INET (host,port) in
  if (!is_server) then
    begin
      Unix.bind sd saddr;
      Unix.listen sd 1;
      while true do
        Format.printf "@[Server Listening...@ ";
        print_saddr saddr;
        Format.printf "@.@]";
        let fd,saddr = Unix.accept sd in
        Format.printf "@[Connection from ";
        print_saddr saddr;
        Format.printf "@.@]";
        chat_logic fd saddr;
        Format.printf "@[Client left, restarting...@.@]";
      done;
      Unix.close sd
    end
  else
    begin
      Format.printf "@[Client Connecting...@ ";
      print_saddr saddr;
      Format.printf "@.@]";
      Unix.connect sd saddr;
      chat_logic sd saddr
    end

let _ =
  Arg.parse args (fun a -> ()) usage;
  main()
