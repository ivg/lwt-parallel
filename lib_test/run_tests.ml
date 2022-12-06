open Lwt
open Lwt.Syntax


type to_sub = [ `Start of int * int array | `Stop ]
type of_sub = float

let to_sub : to_sub Parallel.io =
  Parallel.Io.define
    ~put:(fun chan -> function
        | `Stop ->
          Lwt_io.write_line chan "stop"
        | `Start (name,seed) ->
          Lwt_io.write_line chan "start" >>= fun () ->
          Lwt_io.write_int chan name >>= fun () ->
          Lwt_io.write_int chan (Array.length seed) >>= fun () ->
          Array.to_list seed |>
          Lwt_list.iter_s (Lwt_io.write_int chan))
    ~get:(fun chan ->
        Lwt_io.read_line chan >>= function
        | "stop" -> Lwt.return `Stop
        | "start" ->
          let* name = Lwt_io.read_int chan in
          let* seed_size = Lwt_io.read_int chan in
          let seed = Array.make seed_size 0 in
          List.init seed_size (fun x -> x)|>
          Lwt_list.iter_s (fun i ->
              let+ x = Lwt_io.read_int chan in
              seed.(i) <- x) >|= fun () ->
          `Start (name, seed)
        | _ -> failwith "unknown command")

let of_sub = Parallel.Io.define
    ~put:Lwt_io.write_float64
    ~get:Lwt_io.read_float64

let setup_log level =
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let seed = [| 7; 8; 42; 56 |]
let tasks = 64
let task_size = min (4096 * 1024) (Sys.max_array_length / 2)
let delay = 4. *. atan 1.

let task (data,push) =
  Lwt_stream.next data >>= function
  | `Start (name,seed) ->
    let state = Random.State.make seed in
    Logs_lwt.debug (fun m -> m "<task %03d>: started" name) >>= fun () ->
    Lwt_unix.sleep (Random.float delay) >>= fun () ->
    let array = Array.init task_size (fun _ -> Random.State.float state 1.0) in
    let res = Float.abs @@ Array.fold_left
        (fun acc v -> if Random.State.bool state
          then sin (acc *. v) else cos (acc /. v))
        1.0 array *. 1e5 in
    Logs_lwt.debug (fun m -> m "<task %03d>: computed %g" name res) >>= fun () ->
    return (push (Some res))
  | `Stop -> return (push None)

let spawn_task snapshot (name,time) =
  Lwt_unix.sleep time >>= fun () ->
  let result,command =
    Parallel.process ~snapshot ~out:to_sub ~inc:of_sub task in
  command (Some (`Start (name,seed)));
  command (Some `Stop);
  Lwt_stream.get result

let main_dispatcher snapshot =
  let delays = Array.to_list (Array.init tasks (fun i ->
      i,Random.float delay)) in
  Lwt_list.map_p (spawn_task snapshot) delays >>= function
  | Some r :: rs as total ->
    return (List.length total = tasks && List.for_all (fun r' -> Some r = r') rs)
  | _ -> return_false



let () =
  setup_log (Some Debug);
  let () = Parallel.init () in
  let point1 = Parallel.snapshot () in
  (* let point2 = Parallel.snapshot () in *)
  let test p =
    main_dispatcher p >|= fun r -> assert r in
  Lwt_main.run (test point1);
  print_endline "TESTS DONE"
