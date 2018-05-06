-module(streamfs).

-compile({parse_transform, category}).
-include("streamfs.hrl").
-include_lib("datum/include/datum.hrl").

-export([
   open/2,
   close/1,
   sync/1,
   write/2,
   read/1,
   read/2,
   snapshot/1,
   snapshot/2
]).


%%
%%
-spec open(_, _) -> datum:either( #sfd{} ).

open(File, Modes) ->
   [either ||
      FD <- file:open(File, [raw, read, append, binary | allowed(Modes)]),
      cats:unit(
         #sfd{
            fd    = FD,
            chunk = opts:val(chunk, Modes),
            size  = filelib:file_size(File)
         }
      )
   ].

allowed(Modes) ->
   lists:filter(fun is_allowed/1, Modes).

is_allowed({delayed_write, _, _}) -> true;
is_allowed(delayed_write) -> true;
is_allowed({read_ahead, _}) -> true;
is_allowed(read_ahead) -> true;
is_allowed(_) -> false.


%%
%%
-spec close(#sfd{}) -> datum:either( #sfd{} ).

close(#sfd{fd = FD}) ->
   file:close(FD).

%%
%%
-spec sync(#sfd{}) -> datum:either().

sync(#sfd{fd = FD}) ->
   file:datasync(FD).


%%
%%
-spec write(#sfd{}, datum:stream()) -> datum:either( #sfd{} ).

write(#sfd{fd = FD, size = Size} = File, #stream{} = Stream) ->
   [either ||
      Head =< stream:head(Stream),
      file:write(FD, Head),
      write(File#sfd{size = Size + size(Head)}, stream:tail(Stream))
   ];

write(#sfd{} = File, ?stream()) ->
   {ok, File}.


%%
%%
-spec read(#sfd{}) -> datum:either( datum:stream() ).
-spec read(#sfd{}, forward | reverse) -> datum:either( datum:stream() ).

read(FD) ->
   read(FD, forward).

read(#sfd{} = FD, forward) ->
   {ok, stream:unfold(fun forward/1, #streamfd{fd = FD, at = 0})};

read(#sfd{size = Size} = FD, revesre) ->
   {ok, stream:unfold(fun reverse/1, #streamfd{fd = FD, at = Size})}.


forward(#streamfd{fd = #sfd{fd = FD, chunk = N}, at = At} = Seed) ->
   case file:pread(FD, At, N) of
      {ok, Chunk} ->
         {Chunk, Seed#streamfd{at = At + N}};
      eof ->
         Seed
   end.

reverse(#streamfd{at = 0} = Seed) ->
   Seed;

reverse(#streamfd{fd = #sfd{fd = FD, chunk = N}, at = At} = Seed) ->
   case file:pread(FD, At - N, N) of
      {ok, Chunk} ->
         {Chunk, Seed#streamfd{at = At - N}};
      eof ->
         Seed
   end.

%%
%%
-spec snapshot(#sfd{}) -> datum:either( datum:stream() ).
-spec snapshot(#sfd{}, forward | reverse) -> datum:either( datum:stream() ).

snapshot(FD) ->
   snapshot(FD, forward).

snapshot(#sfd{} = FD, forward) ->
   {ok, stream:unfold(fun snap_forward/1, #streamfd{fd = FD, at = 0})};

snapshot(#sfd{size = Size} = FD, revesre) ->
   {ok, stream:unfold(fun snap_reverse/1, #streamfd{fd = FD, at = Size})}.


snap_forward(#streamfd{fd = #sfd{size = N}, at = At} = Seed)
 when At >= N ->
   Seed;

snap_forward(#streamfd{fd = #sfd{fd = FD, chunk = N}, at = At} = Seed) ->
   case file:pread(FD, At, N) of
      {ok, Chunk} ->
         {Chunk, Seed#streamfd{at = At + N}};
      eof ->
         Seed
   end.

snap_reverse(#streamfd{at = 0} = Seed) ->
   Seed;
   
snap_reverse(#streamfd{fd = #sfd{fd = FD, chunk = N}, at = At} = Seed) ->
   case file:pread(FD, At - N, N) of
      {ok, Chunk} ->
         {Chunk, Seed#streamfd{at = At - N}};
      eof ->
         Seed
   end.
