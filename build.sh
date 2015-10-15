#!/bin/bash
ocamlbuild.native -tag thread -lib unix src/chat.native
