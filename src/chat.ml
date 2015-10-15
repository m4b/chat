(* Compile with:
ocamlbuild.native -tag thread -lib unix src/chat.native
*)
let version = "v1.0.1"

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
    1. a sent message (\000 prefixed),
    2. an acc of a sent message (\001 prefixed)
  Incorrect messages are either bad (too small) or unknown, likely coming from a connection which does not implement this "protocol"
 *)

type msg_kind = | SEND | ACC | BAD | Unknown of int

let show_msg_kind kind =
  match kind with
  | SEND -> "SEND"
  | ACC -> "ACC"
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

(* constructs a simple message with header/kind + contents from a byte sequence *)
let msg_of_bytes bytes len =
  if (len < 2) then
    {kind=BAD; contents=bytes}
  else
    let kind =
      let k = Bytes.get bytes 0 |> Char.code in
      if (k = 0) then
        SEND
      else if (k = 1) then
        ACC
      else Unknown k
    in
    let contents = Bytes.sub bytes 1 (len - 1) in
    {kind; contents}

(* hand-crafted json, if real we'd use jsonm or another json lib, 
  in addition to avoiding ^ string concat, and also appending the acc byte sequence more appropriately *)
let create_acc msg time =
  "\001{acc: \"" ^ msg ^ "\", time: \"" ^ (string_of_float time) ^ "\"}"

(* similar to above, there is a better way of doing this
   (i.e., it's best to keep memory allocations as low as possible when sending),
   but suffices for simple demo *)
let create_send msg =
  let b = Bytes.make (Bytes.length msg + 1) '\000' in
  Bytes.blit msg 0 b 1 (Bytes.length msg);
  b

(* *********************** *)
(* Thread Logic *)
(* *********************** *)

let kBUFFER_SIZE = 4096

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

(* this thread has two responsibilities:
   1. receives bytes and prints them to the console
   2. sends accs on message receipt
 *)

let recv_fn sd saddr mutex running accs =
  let id = Thread.self() |> Thread.id in
  if (!debug) then Format.printf "@[<v 4>@ Recv thread (%d) %s@.@?@]" id (show_saddr saddr);
  let current = ref (Unix.gettimeofday ()) in
  let buffer = Bytes.create kBUFFER_SIZE in
  let prompt = ">" in
  try
    while !running do
      let ready = Thread.wait_timed_read sd 0.1 in
      if (ready) then
        begin
          let buffer,count = consume_msg sd buffer in
          if (!debug) then Format.printf "@[<v 4>@ RECV count: %d msg: %s@.@?@]" count (Bytes.sub_string buffer 0 count);
          if (count <= 0) then
            running := false
          else
            begin
              let msg = msg_of_bytes buffer count in
              if (!debug) then
                begin
                  Format.printf "@[<v 4>@ MSG is %a@.@]" pp_msg msg;
                  Format.printf "@[<v 4>@ COUNT %d@.@]" (Bytes.length msg.contents);
                end;
              let time = Unix.gettimeofday () in
              current := time -. !current;
              match msg.kind with
              | ACC ->
                 begin
                   Mutex.lock mutex;
                   let sentmsg = Stack.pop accs in
                   Mutex.unlock mutex;
                   if (!debug) then
                     begin
                       (* super hacky, again need proper json lib *)
                       try
                         let size = Bytes.length sentmsg in
                         (*                          let size = min (Bytes.length sentmsg) ((Bytes.length msg.contents) - 7) in *)
                         let extract = Bytes.sub_string msg.contents 7 size in
                         Format.printf "@[<v 4>@ DBG: received acc \"%s\" from sent message \"%s\" with roundtrip preservation: %b@.@]"
                                       extract sentmsg (extract = sentmsg)
                       with _ -> Format.eprintf "@[<v 4>@ <WARN>: could not extract acc from msg %s@.@]" msg.contents
                     end;
                   Format.printf "@[<v 2>@ << %s@.@]@[%s @]@?" msg.contents prompt;
                 end
              | SEND ->
                 begin
                   Format.printf "@[@.< %s@.@]@[%s @]@?" msg.contents prompt;
                   let acc = create_acc msg.contents time in
                   ignore @@ Unix.send sd acc 0 (Bytes.length acc) []
                 end
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
  with exn ->
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
      let ready = Thread.wait_timed_read (Unix.descr_of_in_channel stdin) 0.1 in
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
              if (!debug) then
                Format.printf
                  "@[<v 4>@ SEND count %d@.@?@]"
                  (Bytes.length msg);
              ignore @@ Unix.send sd msg 0 (Bytes.length msg) [];
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
