(** Typesafe multiprocessing.  *)

type 'a io

(** [init ()] should be called as soon as possible  *)
val init: unit -> unit

(** {4 Simple interface}  *)

(** [run f] runs [f] in different process. *)
val run:
  ?inc:'inc io ->
  ?out:'out io ->
  ('out -> 'inc Lwt.t) -> ('out -> 'inc Lwt.t)

(** {4 Expert interface}  *)

(** Type safe pipe, with ['a] read end and ['b] write end*)
type ('a,'b) pipe = 'a Lwt_stream.t * ('b option -> unit)

(** [create process] executes function [process] in other process.  *)
val process:
  ?inc:'inc io ->
  ?out:'out io ->
  (('out,'inc) pipe -> unit Lwt.t) -> ('inc,'out) pipe


module Io : sig
  val define :
    put:(Lwt_io.output_channel -> 'a -> unit Lwt.t) ->
    get:(Lwt_io.input_channel -> 'a Lwt.t) -> 'a io

  val put : 'a io -> Lwt_io.output_channel -> 'a -> unit Lwt.t
  val get : 'a io -> Lwt_io.input_channel -> 'a Lwt.t

  val marshaling : 'a io
end
