(** Typesafe multiprocessing.  *)

val init: unit -> unit
(** [init ()] should be called as soon as possible  *)

(** {4 Simple interface}  *)

val run: ('a -> 'b Lwt.t) -> ('a -> 'b Lwt.t)
(** [run f] runs [f] in different process. *)

(** {4 Expert interface}  *)

(** Type safe pipe, with ['a] read end and ['b] write end*)
type ('a,'b) pipe = 'a Lwt_stream.t * ('b option -> unit)

val process: (('a,'b) pipe -> unit Lwt.t) -> ('b,'a) pipe
(** [create process] executes function [process] in other process.  *)
