# Cancelling isn't possible while curl is running a callback, and garbage
# collection can happen at any time, so finalizers must defer it.
multi_cancel_later <- function(handle, on_cancel = NULL) {
  cancel <- function() {
    # The handle might have been finalized along with the body
    tryCatch(curl::multi_cancel(handle), error = function(cnd) NULL)
    if (!is.null(on_cancel)) {
      on_cancel()
    }
  }
  later::later(cancel, loop = later::global_loop())
  invisible()
}

# A StreamingBody that's filled by a curl handle running in a pool. Bytes are
# pushed into a queue by curl's data callback (driven by the pool poller in
# the background), and read from the queue by the resp_stream_*() functions.
# Reading also pumps the pool, so the stream makes progress even if the event
# loop isn't running. If the body is garbage collected before the transfer is
# complete, the transfer is cancelled.
MultiStreamingBody <- R6::R6Class(
  "MultiStreamingBody",
  inherit = StreamingBody,
  public = list(
    initialize = function(handle, pool = NULL, on_cancel = NULL) {
      private$handle <- handle
      private$pool <- pool
      private$on_cancel <- on_cancel
    },

    read = function(n) {
      if (!private$complete && !private$closed) {
        curl::multi_run(timeout = 0, pool = private$pool)
      }

      if (private$n_buffered == 0) {
        if (!is.null(private$error)) {
          error <- private$error
          private$error <- NULL
          cnd_signal(error)
        }
        return(raw())
      }

      n <- min(n, private$n_buffered)
      out <- vector("list", 0)
      needed <- n
      while (needed > 0) {
        chunk <- private$chunks[[private$head]]
        available <- length(chunk) - private$offset
        take <- min(available, needed)
        out[[length(out) + 1L]] <- chunk[private$offset + seq_len(take)]
        needed <- needed - take
        if (take == available) {
          private$chunks[private$head] <- list(NULL)
          private$head <- private$head + 1L
          private$offset <- 0L
        } else {
          private$offset <- private$offset + take
        }
      }
      private$n_buffered <- private$n_buffered - n
      private$compact()

      unlist(out, recursive = FALSE, use.names = FALSE)
    },

    # Waits for the transfer to finish, rather than spinning on read()
    read_all = function(buffer = 32 * 1024) {
      while (!private$complete && !private$closed) {
        curl::multi_run(timeout = 0.1, pool = private$pool)
      }
      super$read_all(buffer)
    },

    is_open = function() {
      !private$closed
    },

    # Not complete until any error has been surfaced by read()
    is_complete = function() {
      private$complete && private$n_buffered == 0 && is.null(private$error)
    },

    get_fdset = function() {
      fds <- curl::multi_fdset(pool = private$pool)
      # Once the transfer is over there's nothing to wait for
      if (private$complete) {
        fds$timeout <- 0
      }
      fds
    },

    close = function() {
      if (private$closed) {
        return(invisible())
      }
      private$closed <- TRUE
      if (!private$complete) {
        private$complete <- TRUE
        curl::multi_cancel(private$handle)
        if (!is.null(private$on_cancel)) {
          private$on_cancel()
        }
      }
      private$chunks <- list()
      private$head <- 1L
      private$offset <- 0L
      private$n_buffered <- 0
      invisible()
    },

    # Called by the producer -------------------------------------------------

    push = function(bytes) {
      if (private$closed || length(bytes) == 0) {
        return(invisible())
      }
      private$chunks[[length(private$chunks) + 1L]] <- bytes
      private$n_buffered <- private$n_buffered + length(bytes)
      invisible()
    },

    finish = function(error = NULL) {
      private$complete <- TRUE
      private$error <- error
      invisible()
    }
  ),
  private = list(
    handle = NULL,
    pool = NULL,
    on_cancel = NULL,

    chunks = list(),
    head = 1L,
    offset = 0L,
    n_buffered = 0,

    complete = FALSE,
    closed = FALSE,
    error = NULL,

    finalize = function() {
      if (!private$complete && !private$closed) {
        multi_cancel_later(private$handle, private$on_cancel)
      }
    },

    # Drop consumed chunks once they make up most of the list
    compact = function() {
      if (private$head > 32L && private$head > length(private$chunks) / 2) {
        private$chunks <- private$chunks[-seq_len(private$head - 1L)]
        private$head <- 1L
      }
    }
  )
)
