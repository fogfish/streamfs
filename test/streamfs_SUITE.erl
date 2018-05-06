-module(streamfs_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile({parse_transform, category}).

-export([all/0]).
-export([
   lifecycle/1,
   write/1,
   read/1,
   snapshot/1
]).


all() ->
   [
      lifecycle,
      write,
      read,
      snapshot
   ].

%%
%%
lifecycle(_) ->
   [either ||
      streamfs:open("/tmp/lifecycle", [raw, {chunk, 1}]),
      streamfs:close(_)
   ].

%%
%%
write(_) ->
   {ok, <<"abc">>} = [either ||
      cats:unit(file:delete("/tmp/writeable")),

      FD <- streamfs:open("/tmp/writeable", [raw, {chunk, 1}]),
      streamfs:write(FD, stream:build([<<"a">>, <<"b">>, <<"c">>])),
      streamfs:close(FD),
      file:read_file("/tmp/writeable")
   ].

%%
%%
read(_) ->
   {ok, [<<"a">>, <<"b">>, <<"c">>]} = [either ||
      cats:unit(file:delete("/tmp/readable")),

      FD0 <- streamfs:open("/tmp/readable", [raw, {chunk, 1}]),
      FD1 <- streamfs:write(FD0, stream:build([<<"a">>, <<"b">>, <<"c">>])),
      Stream <- streamfs:read(FD1),
      Data <- cats:unit(stream:list(Stream)),
      streamfs:close(FD1),
      cats:unit(Data)
   ].

%%
%%
snapshot(_) ->
   {ok, [<<"a">>, <<"b">>, <<"c">>]} = [either ||
      cats:unit(file:delete("/tmp/snapshot")),

      FD0 <- streamfs:open("/tmp/snapshot", [raw, {chunk, 1}]),
      FD1 <- streamfs:write(FD0, stream:build([<<"a">>, <<"b">>, <<"c">>])),
      Stream <- streamfs:snapshot(FD1),
      FD2 <- streamfs:write(FD1, stream:build([<<"0">>, <<"1">>, <<"2">>])),      
      Data <- cats:unit(stream:list(Stream)),
      streamfs:close(FD2),
      cats:unit(Data)
   ].


