#' Perform a request asynchronously and return a streaming connection
#'
#' @description
#' `r lifecycle::badge("experimental")`
#'
#' This variation on [req_perform_connection()] returns a [promises::promise()]
#' immediately and opens the connection in the background, so the event loop
#' stays responsive while waiting for the server to respond. The promise
#' resolves to a streaming [response] as soon as the response headers have
#' been received. You can then use [resp_stream_raw()], [resp_stream_lines()],
#' [resp_stream_sse()], or [resp_stream_aws()] to read the body. Reads never
#' block: if no data is available yet, they return an empty result. Functions
#' that read the whole body, such as [resp_body_string()], wait for the
#' transfer to finish and block the event loop while they wait. Always finish
#' up by calling `close(response)`.
#'
#' Unlike [req_perform_connection()], the body is received in the background
#' as fast as the server sends it and is held in memory until you read it. If
#' you read more slowly than the server sends, memory usage will grow, so call
#' `close(response)` as soon as you no longer need the stream.
#'
#' If the server returns an HTTP error, the promise rejects with the same
#' error that [req_perform_connection()] would throw, after the complete error
#' body has been received.
#'
#' If using together with [later::with_temp_loop()] or other private event
#' loops, a new curl pool made by [curl::new_pool()] should be created for
#' requests made within the loop to ensure that only these requests are being
#' polled by the loop.
#'
#' Like [req_perform_promise()], `req_perform_connection_promise()`:
#'
#' * Will not retrieve a new OAuth token if it expires after the promised
#'   request is created but before it is actually requested.
#' * Does not perform throttling with [req_throttle()].
#' * Does not attempt retries as described by [req_retry()].
#'
#' @inheritParams req_perform_connection
#' @inheritParams req_perform_promise
#' @return A [promises::promise()] that resolves to a streaming [response] if
#'   successful or rejects on the same errors thrown by
#'   [req_perform_connection()].
#' @export
#' @examples
#' \dontrun{
#' req <- request(example_url()) |>
#'   req_template("GET /stream/:n", n = 5)
#'
#' p <- req_perform_connection_promise(req)
#' promises::then(p, function(resp) {
#'   lines <- resp_stream_lines(resp, Inf)
#'   cat(length(lines), " lines received so far\n", sep = "")
#'   close(resp)
#' })
#' }
req_perform_connection_promise <- function(
  req,
  pool = NULL,
  verbosity = NULL,
  mock = getOption("httr2_mock", NULL)
) {
  check_installed(c("promises", "later"))

  check_request(req)
  check_promise_pool(pool, missing(pool))
  mock <- as_mock_function(mock)
  req <- req_verbosity_connection(req, verbosity %||% httr2_verbosity())
  req <- req_policies(req, connection = TRUE)
  error_call <- current_env()

  promises::promise(function(resolve, reject) {
    if (!is.null(mock)) {
      mock_resp <- mock(req)
      if (!is.null(mock_resp)) {
        tryCatch(
          resolve(handle_resp(req, mock_resp, error_call = error_call)),
          error = function(cnd) reject(cnd)
        )
        return()
      }
    }

    connection_promise_submit(req, pool, resolve, reject, error_call)
    ensure_pool_poller(pool)
  })
}

# Adds the request to the pool and wires curl's callbacks up to the promise.
# The promise resolves when the headers of the final response arrive. curl has
# no callback for that, so the pool poller checks the handle each time it runs
# (see `pool_add_watcher()`); as a fallback we also resolve when the first
# body bytes arrive or when the request completes. HTTP errors are only
# rejected once the complete body has arrived, so that it can be included in
# the error.
connection_promise_submit <- function(req, pool, resolve, reject, error_call) {
  # An unforced argument would keep the caller's frame, and so the promise
  # and its value, alive
  force(error_call)

  req_prep <- req_prepare(req)
  handle <- req_handle(req_prep)
  # Pooled request spans should not become the active span
  req_prep <- req_with_span(req_prep, activate = FALSE)
  handle_preflight(req_prep, handle)
  the$last_request <- req
  the$last_response <- NULL
  signal(class = "httr2_perform_connection")

  # The transfer can finish after a dropped body has scheduled its cancel
  completed <- FALSE
  complete <- function() {
    if (!completed) {
      completed <<- TRUE
      req_completed(req_prep)
    }
  }

  body <- MultiStreamingBody$new(
    handle,
    pool,
    on_cancel = function() {
      complete()
      req_record_span_status(req_prep)
      pool_wake_poller(pool)
    }
  )
  weak_body <- new_weakref(body)
  resp <- NULL
  state <- "pending"

  # curl's callbacks live as long as the transfer does, so once the promise
  # settles they must only hold a weak reference to the body. That way a
  # response that's dropped without being closed can be garbage collected,
  # which cancels the transfer.
  settle <- function(new_state, fun, value) {
    force(fun)
    force(value)
    state <<- new_state
    body <<- resp <<- resolve <<- reject <<- NULL
    fun(value)
  }
  do_reject <- function(cnd) {
    if (state == "pending") {
      settle("rejected", reject, cnd)
    }
  }
  get_body <- function() body %||% wref_key(weak_body)

  receive_headers <- function(curl_data) {
    resp <<- create_response(req, curl_data, body)
    if (!error_is_error(req, resp)) {
      req_record_span_status(req_prep, resp)
      settle("resolved", resolve, resp)
    }
  }
  try_receive_headers <- function(curl_data) {
    tryCatch(
      receive_headers(curl_data),
      error = function(cnd) {
        # Can't cancel the transfer from inside curl's callbacks
        body <- body
        later::later(function() body$close())
        do_reject(cnd)
      }
    )
  }

  # Called by the pool poller; returns `TRUE` when there's nothing more to do
  check_headers <- function() {
    if (state != "pending" || !is.null(resp)) {
      return(TRUE)
    }
    curl_data <- curl::handle_data(handle)
    if (!has_final_headers(curl_data)) {
      return(FALSE)
    }
    try_receive_headers(curl_data)
    TRUE
  }

  on_data <- function(bytes, final = FALSE) {
    if (length(bytes) == 0 || state == "rejected") {
      return()
    }
    if (state == "pending" && is.null(resp)) {
      try_receive_headers(curl::handle_data(handle))
      if (state == "rejected") {
        return()
      }
    }
    body <- get_body()
    if (!is.null(body)) {
      body$push(bytes)
    }
  }

  on_done <- function(curl_data) {
    complete()
    body <- get_body()
    if (!is.null(body)) {
      body$finish()
    }
    if (state != "pending") {
      return()
    }

    tryCatch(
      {
        # Any headers we've already seen might not be from the final response
        receive_headers(curl_data)
        if (state == "pending") {
          resp$body <- body$read_all()
          the$last_response <- resp
          req_record_span_status(req_prep, resp)
          do_reject(resp_failure_cnd(req, resp, error_call = error_call))
        }
      },
      error = function(cnd) do_reject(cnd)
    )
  }

  on_fail <- function(msg) {
    complete()

    error_class <- setdiff(class(msg), "character")
    curl_error <- error_cnd(message = msg, class = error_class, call = NULL)
    error <- curl_cnd(curl_error, call = error_call)
    error$request <- req

    # If we've already resolved, surface the error on the next read
    body <- get_body()
    if (!is.null(body)) {
      body$finish(error)
    }
    if (state == "pending") {
      req_record_span_status(req_prep, error)
      do_reject(error)
    }
  }

  curl_multi_add(
    handle = handle,
    pool = pool,
    data = on_data,
    done = on_done,
    fail = on_fail
  )
  pool_add_watcher(pool, check_headers)
  invisible()
}

# Have we received the complete headers of the final response? curl records
# the headers of every response (including redirects, auth challenges, and
# 1xx), so we only trust a status that can't be followed by another request.
# This mirrors the logic that curl uses when opening a connection.
has_final_headers <- function(curl_data) {
  status <- curl_data$status_code
  if (status < 200 || (status >= 300 && status < 400)) {
    return(FALSE)
  }
  if (status %in% c(401, 407)) {
    return(FALSE)
  }

  # Headers end with a blank line
  lf <- as.raw(0x0a)
  cr <- as.raw(0x0d)
  headers <- curl_data$headers
  n <- length(headers)
  if (n < 2 || headers[[n]] != lf) {
    return(FALSE)
  }
  headers[[n - 1]] == lf ||
    (n >= 3 && headers[[n - 1]] == cr && headers[[n - 2]] == lf)
}
