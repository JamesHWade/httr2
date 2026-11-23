#' Perform request asynchronously using the promises package
#'
#' @description
#' `r lifecycle::badge("experimental")`
#'
#' This variation on [req_perform()] returns a [promises::promise()] object immediately
#' and then performs the request in the background, returning program control before the request
#' is finished. See the
#' [promises package documentation](https://rstudio.github.io/promises/articles/promises_01_motivation.html)
#' for more details on how to work with the resulting promise object.
#'
#' If using together with [later::with_temp_loop()] or other private event loops,
#' a new curl pool made by [curl::new_pool()] should be created for requests made
#' within the loop to ensure that only these requests are being polled by the loop.
#'
#' Like with [req_perform_parallel()], exercise caution when using this function;
#' it's easy to pummel a server with many simultaneous requests. Also, not all servers
#' can handle more than 1 request at a time, so the responses may still return
#' sequentially.
#'
#' `req_perform_promise()` also has similar limitations to the
#' [req_perform_parallel()] function, it:
#'
#' * Will not retrieve a new OAuth token if it expires after the promised request
#'   is created but before it is actually requested.
#' * Does not perform throttling with [req_throttle()].
#' * Does not attempt retries as described by [req_retry()].
#' * Only consults the cache set by [req_cache()] when the request is promised.
#'
#' @inheritParams req_perform
#' @inheritParams req_perform_parallel
#' @param pool A pool created by [curl::new_pool()].
#' @return a [promises::promise()] object which resolves to a [response] if
#' successful or rejects on the same errors thrown by [req_perform()].
#' @export
#'
#' @examples
#' \dontrun{
#' library(promises)
#' request_base <- request(example_url()) |> req_url_path_append("delay")
#'
#' p <- request_base |> req_url_path_append(2) |> req_perform_promise()
#'
#' # A promise object, not particularly useful on its own
#' p
#'
#' # Use promise chaining functions to access results
#' p %...>%
#'   resp_body_json() %...>%
#'   print()
#'
#'
#' # Can run two requests at the same time
#' p1 <- request_base |> req_url_path_append(2) |> req_perform_promise()
#' p2 <- request_base |> req_url_path_append(1) |> req_perform_promise()
#'
#' p1 %...>%
#'   resp_url_path %...>%
#'   paste0(., " finished") %...>%
#'   print()
#'
#' p2 %...>%
#'   resp_url_path %...>%
#'   paste0(., " finished") %...>%
#'   print()
#'
#' # See the [promises package documentation](https://rstudio.github.io/promises/)
#' # for more information on working with promises
#' }
req_perform_promise <- function(
  req,
  path = NULL,
  pool = NULL,
  verbosity = NULL,
  mock = getOption("httr2_mock", NULL)
) {
  check_installed(c("promises", "later"))

  check_request(req)
  check_string(path, allow_null = TRUE)
  verbosity <- verbosity %||% httr2_verbosity()
  mock <- as_mock_function(mock)
  check_promise_pool(pool, missing(pool))
  # verbosity checked by req_verbosity
  req <- req_verbosity(req, verbosity)

  promises::promise(function(resolve, reject) {
    pooled_req <- pooled_request(
      req = req,
      path = path,
      on_success = function(resp) resolve(resp),
      on_failure = function(error) reject(error),
      on_error = function(error) reject(error),
      mock = mock
    )
    pooled_req$submit(pool)
    ensure_pool_poller(pool)
  })
}

check_promise_pool <- function(pool, missing_pool, call = caller_env()) {
  if (missing_pool) {
    if (!identical(later::current_loop(), later::global_loop())) {
      cli::cli_abort(
        c(
          "Must supply {.arg pool} when calling {.code later::with_temp_loop()}.",
          i = "Do you need {.code pool = curl::new_pool()}?"
        ),
        call = call
      )
    }
  } else if (!is.null(pool) && !inherits(pool, "curl_multi")) {
    stop_input_type(pool, "a {curl} pool", allow_null = TRUE, call = call)
  }
}

ensure_pool_poller <- function(pool) {
  key <- obj_address(pool)
  wake <- the$pool_pollers[[key]]
  if (!is.null(wake) && wake()) {
    return()
  }

  loop <- later::current_loop()
  cancel_wait <- NULL
  poll_pool <- function(ready) {
    cancel_wait <<- NULL
    withCallingHandlers(
      {
        curl::multi_run(0, pool = pool)
        pool_run_watchers(pool)
        # A completion callback may have added another handle.
        if (length(curl::multi_list(pool = pool)) > 0) {
          fds <- curl::multi_fdset(pool = pool)
          cancel_wait <<- later::later_fd(
            func = poll_pool,
            readfds = fds$reads,
            writefds = fds$writes,
            exceptfds = fds$exceptions,
            # curl reports milliseconds and later expects seconds.
            timeout = fds$timeout / 1000,
            loop = loop
          )
        } else {
          env_unbind(the$pool_pollers, key)
        }
      },
      error = function(cnd) {
        pool_clear_watchers(pool)
        env_unbind(the$pool_pollers, key)
      }
    )
  }

  the$pool_pollers[[key]] <- function() {
    alive <- later::exists_loop(loop)
    if (!is.null(cancel_wait)) {
      cancel_wait()
      cancel_wait <<- NULL
      if (alive) {
        later::later(poll_pool, loop = loop)
      }
    }
    # A running or scheduled poll will include the new request. If its loop
    # was destroyed, let the caller start a new poller on the current loop.
    alive
  }
  poll_pool()
}

# Watchers are functions called each time the poller has run the pool, for
# things that curl doesn't provide a callback for. A watcher returns `TRUE`
# once it's finished and no longer needs to be called.
pool_add_watcher <- function(pool, watcher) {
  key <- obj_address(pool)
  the$pool_watchers[[key]] <- c(the$pool_watchers[[key]], list(watcher))
  invisible()
}

pool_run_watchers <- function(pool) {
  key <- obj_address(pool)
  watchers <- the$pool_watchers[[key]]
  if (length(watchers) == 0) {
    return(invisible())
  }

  pool_clear_watchers(pool)
  finished <- map_lgl(watchers, function(watcher) {
    tryCatch(isTRUE(watcher()), error = function(cnd) TRUE)
  })
  # A watcher might have added new watchers
  remaining <- c(watchers[!finished], the$pool_watchers[[key]])
  if (length(remaining) > 0) {
    the$pool_watchers[[key]] <- remaining
  }
  invisible()
}

pool_clear_watchers <- function(pool) {
  env_unbind(the$pool_watchers, obj_address(pool))
}
