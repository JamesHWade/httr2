# promises package test helper
extract_promise <- function(promise, timeout = 30) {
  promise_value <- NULL
  error <- NULL
  promises::then(
    promise,
    onFulfilled = function(value) promise_value <<- value,
    onRejected = function(reason) error <<- reason
  )

  start <- Sys.time()
  while (!later::loop_empty()) {
    if (difftime(Sys.time(), start, units = "secs") > timeout) {
      stop("Waited too long")
    }
    later::run_now(0.01)
  }

  if (!is.null(error)) {
    cnd_signal(error)
  } else {
    promise_value
  }
}

# Like extract_promise(), but returns as soon as the promise settles, rather
# than waiting for the event loop to empty. Needed for streaming connections,
# where the pool keeps being polled until the body is complete.
await_promise <- function(promise, timeout = 30) {
  settled <- FALSE
  promise_value <- NULL
  error <- NULL
  promises::then(
    promise,
    onFulfilled = function(value) {
      settled <<- TRUE
      promise_value <<- value
    },
    onRejected = function(reason) {
      settled <<- TRUE
      error <<- reason
    }
  )

  start <- Sys.time()
  while (!settled) {
    if (difftime(Sys.time(), start, units = "secs") > timeout) {
      stop("Waited too long")
    }
    later::run_now(0.01)
  }

  if (!is.null(error)) {
    cnd_signal(error)
  } else {
    promise_value
  }
}
