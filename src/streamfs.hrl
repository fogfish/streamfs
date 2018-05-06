
-record(sfd, {
   fd    = undefined :: _,   %% file descriptor
   chunk = undefined :: _,   %% stream chunk (unit of work)
   size  = undefined :: _    %% size of stream in bytes 
}).

-record(streamfd, {
   fd    = undefined :: _,
   at    = undefined :: _ 
}).