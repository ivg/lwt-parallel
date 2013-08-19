
open Lwt
open Lwt_log

let setup_logger () =
  let logger = Lwt_log.channel
    (* ~facility:`User *)
    ~close_mode:`Keep
    ~template:"parallax-$pid:$(date).$(milliseconds) $(section): $(message)"
    ~channel:Lwt_io.stderr
    (* ~file_name:"parallax.log" *)
    () in
  Lwt_log.default := logger

let _ = setup_logger ()
let _ = Parallel.init ()


let simpliest_test () =
  ign_notice "Starting simple test";
  let sum = (Parallel.run (fun (a,b) ->
      ign_notice_f "executing sum %g + %g" a b;
      return (a +. b))) in
  let sum' = Parallel.run (fun (a,b) -> return (a +. b)) in
  lwt r = sum' (12.,14.) in
  lwt s = sum  (13.,15.) in
  Lwt_io.printf "a + b = %g > c + d = %g\n" r s

let stream_test () =
  let rec exec (data,push) =
    Lwt_stream.iter_s (fun size ->
      let nums = Array.init size (fun n -> n) in
      return (Array.iter (fun n -> push (Some n)) nums)) data in
  let results,push = Parallel.process exec in
  let sizes = Array.init 100 (fun n -> n + 1) in
  Array.iter (fun n -> push (Some n)) sizes;
  push None;
  Lwt_stream.iter_s (fun n ->
      if n + 1 = 100
      then Lwt_io.printf "finishing\n"
      else return ()) results

let process_in_process () =
  let sum = (
    Parallel.run (fun (a,b) ->
        let plus = Parallel.run (fun b -> return (a +. b)) in
        plus b)) in
  sum (12.,13.) >>= Lwt_io.printf "%g\n"

let _ = Lwt_main.run (
    join [simpliest_test (); stream_test (); simpliest_test ()]
    >>
    simpliest_test ()
    >>
    stream_test ()
    >> Lwt_io.flush_all ())

let _ = Lwt_main.run (
    join [process_in_process (); simpliest_test (); stream_test (); simpliest_test ()] >>
    process_in_process () >>
    simpliest_test () >>
    process_in_process () >>
    process_in_process () >>
    stream_test () >>
    process_in_process () >>
    process_in_process () >>
    Lwt_io.flush_all ())
