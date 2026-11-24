test_that("checks its inputs", {
  req <- request_test()

  expect_snapshot(error = TRUE, {
    req_perform_connection_promise(1)
    req_perform_connection_promise(req, pool = "INVALID")
    req_perform_connection_promise(req, verbosity = "INVALID")
  })
})

test_that("event loop isn't blocked while waiting for headers (#921)", {
  req <- local_app_request(function(req, res) {
    Sys.sleep(1)
    res$send_json(list(ok = TRUE), auto_unbox = TRUE)
  })

  start <- Sys.time()
  fired <- NULL
  later::later(function() fired <<- Sys.time(), 0.1)
  expect_condition(
    p <- req_perform_connection_promise(req),
    class = "httr2_perform_connection"
  )
  expect_s3_class(p, "promise")
  resp <- await_promise(p)
  withr::defer(close(resp))
  resolved <- Sys.time()

  expect_lt(as.double(fired - start, units = "secs"), 0.5)
  expect_gte(as.double(resolved - start, units = "secs"), 1)
  expect_equal(resp_status(resp), 200)
  expect_s3_class(resp$body, "StreamingBody")
  expect_equal(resp_body_json(resp), list(ok = TRUE))
})

test_that("resolves once the headers are received", {
  req <- local_app_request(function(req, res) {
    if (is.null(res$locals$sent)) {
      res$locals$sent <- TRUE
      res$set_header("Content-Length", 5)$set_type("text/plain")
      res$write("")
      res$delay(1)
    } else {
      res$write("hello")
      res$send("")
    }
  })

  resp <- await_promise(req_perform_connection_promise(req))
  withr::defer(close(resp))
  expect_equal(resp_status(resp), 200)
  expect_equal(resp_header(resp, "Content-Length"), "5")
  expect_equal(resp_stream_raw(resp), raw())

  expect_equal(resp_body_string(resp), "hello")
})

test_that("can stream lines and server-sent events", {
  resp <- await_promise(
    req_perform_connection_promise(request_test("/stream/:n", n = 5))
  )
  withr::defer(close(resp))
  lines <- character()
  while (!resp_stream_is_complete(resp)) {
    lines <- c(lines, resp_stream_lines(resp))
  }
  expect_length(lines, 5)

  resp <- await_promise(
    req_perform_connection_promise(stream_app_chunks(
      "data: 1\n\n",
      "data: 2\n\n"
    ))
  )
  withr::defer(close(resp))
  events <- list()
  while (!resp_stream_is_complete(resp)) {
    event <- resp_stream_sse(resp)
    if (!is.null(event)) events[[length(events) + 1]] <- event$data
  }
  expect_equal(events, list("1", "2"))
})

test_that("reads don't block", {
  sync <- sync_req("pull")
  resp <- await_promise(
    req_perform_connection_promise(stream_app_request("/pull"))
  )
  withr::defer(close(resp))

  expect_equal(resp_stream_lines(resp), character())
  expect_equal(resp_stream_is_complete(resp), FALSE)

  sync(resp)
  expect_equal(resp_stream_lines(resp), "This is a complete sentence.")
})

test_that("can wait for data with get_fdset()", {
  req <- request_test("/drip?duration=0.5&numbytes=5")
  resp <- await_promise(req_perform_connection_promise(req))
  withr::defer(close(resp))

  bytes <- raw()
  n_reads <- integer()
  while (!resp_stream_is_complete(resp)) {
    bytes <- c(bytes, resp_stream_raw(resp))
    fds <- resp$body$get_fdset()
    n_reads <- c(n_reads, length(fds$reads))
    wait <- promises::promise(function(resolve, reject) {
      later::later_fd(
        resolve,
        fds$reads,
        fds$writes,
        fds$exceptions,
        fds$timeout / 1000
      )
    })
    await_promise(wait, timeout = 5)
  }
  expect_length(bytes, 5)
  expect_gt(max(n_reads), 0)
})

test_that("handles empty bodies", {
  resp <- await_promise(
    req_perform_connection_promise(request_test("/status/:s", s = 204))
  )
  expect_equal(resp_status(resp), 204)
  expect_equal(resp_stream_is_complete(resp), TRUE)
  expect_equal(resp_stream_raw(resp), raw())
  close(resp)
})

test_that("follows redirects", {
  resp <- await_promise(
    req_perform_connection_promise(request_test("/redirect/:n", n = 2))
  )
  withr::defer(close(resp))
  expect_equal(resp_url_path(resp), "/get")
})

test_that("HTTP errors reject with the complete body", {
  req <- local_app_request(function(req, res) {
    res$set_status(404L)$send_json(list(status = 404), auto_unbox = TRUE)
  })

  cnd <- expect_error(
    await_promise(req_perform_connection_promise(req)),
    class = "httr2_http_404"
  )
  expect_equal(resp_body_json(cnd$resp), list(status = 404))
  expect_equal(resp_body_json(last_response()), list(status = 404))
})

test_that("curl errors reject", {
  local_curl_error()
  req <- request("http://127.0.0.1")

  cnd <- expect_error(
    await_promise(req_perform_connection_promise(req)),
    class = "httr2_failure"
  )
  expect_equal(cnd$request, req_policies(req, connection = TRUE))
})

test_that("failures after resolving are surfaced when reading", {
  req <- local_app_request(function(req, res) {
    res$set_header("Content-Length", 100)$set_type("text/plain")
    res$send("short")
  })

  resp <- await_promise(req_perform_connection_promise(req))
  withr::defer(close(resp))

  out <- raw()
  expect_error(
    while (!resp_stream_is_complete(resp)) {
      out <- c(out, resp_stream_raw(resp))
      later::run_now(0.01)
    },
    class = "httr2_failure"
  )
  expect_equal(rawToChar(out), "short")
  expect_equal(resp_stream_is_complete(resp), TRUE)
})

test_that("close() cancels an in-flight transfer", {
  pool <- curl::new_pool()
  req <- request_test("/drip?duration=5&numbytes=5")
  resp <- await_promise(req_perform_connection_promise(req, pool = pool))
  expect_length(curl::multi_list(pool), 1)

  close(resp)
  expect_length(curl::multi_list(pool), 0)
  expect_equal(resp$body$is_open(), FALSE)
  expect_snapshot(resp_stream_raw(resp), error = TRUE)
})

test_that("close() stops the pool poller", {
  pool <- curl::new_pool()
  req <- request_test("/drip?duration=5&numbytes=5")
  resp <- await_promise(req_perform_connection_promise(req, pool = pool))

  close(resp)
  later::run_now()
  expect_null(the$pool_pollers[[obj_address(pool)]])
})

test_that("dropping an unclosed response cancels the transfer", {
  pool <- curl::new_pool()
  req <- request_test("/drip?duration=5&numbytes=5")
  resp <- await_promise(req_perform_connection_promise(req, pool = pool))
  expect_length(curl::multi_list(pool), 1)

  rm(resp)
  the$last_response <- NULL
  # Let the event loop release the settled promise
  later::run_now()
  gc()
  later::run_now()
  expect_length(curl::multi_list(pool), 0)
})

test_that("dropping an unclosed response in a temporary loop cancels on that loop", {
  pool <- curl::new_pool()
  req <- request_test("/drip?duration=5&numbytes=5")

  later::with_temp_loop({
    resp <- await_promise(req_perform_connection_promise(req, pool = pool))
    expect_length(curl::multi_list(pool), 1)

    rm(resp)
    the$last_response <- NULL
    later::run_now()
    gc()
    later::run_now()
    expect_length(curl::multi_list(pool), 0)
    later::run_now()
    expect_null(the$pool_pollers[[obj_address(pool)]])
  })

  # Falls back to the global loop once the poller's loop is destroyed
  later::with_temp_loop({
    resp <- await_promise(req_perform_connection_promise(req, pool = pool))
    later::run_now()
  })
  expect_length(curl::multi_list(pool), 1)

  rm(resp)
  the$last_response <- NULL
  gc()
  later::run_now()
  expect_length(curl::multi_list(pool), 0)
})

test_that("done policy runs once when a dropped response completes", {
  pool <- curl::new_pool()
  n_done <- 0
  req <- request_test("/drip?duration=0.2&numbytes=2") |>
    req_policies(done = function() n_done <<- n_done + 1)
  resp <- await_promise(req_perform_connection_promise(req, pool = pool))

  rm(resp)
  the$last_response <- NULL
  later::run_now()
  gc()
  # Finish the transfer before the deferred cancel gets to run
  curl::multi_run(pool = pool)
  later::run_now()
  expect_equal(n_done, 1)
})

test_that("can start a new request after closing another", {
  resp <- await_promise(
    req_perform_connection_promise(request_test("/drip?duration=5&numbytes=5"))
  )
  close(resp)

  start <- Sys.time()
  resp <- await_promise(req_perform_connection_promise(request_test("/get")))
  close(resp)
  expect_lt(as.double(Sys.time() - start, units = "secs"), 0.5)
})

test_that("must supply pool in temporary loop", {
  later::with_temp_loop({
    expect_snapshot(
      req_perform_connection_promise(request_test()),
      error = TRUE
    )
  })
})

test_that("mocking works", {
  local_mocked_responses(function(req) {
    expect_equal(req$policies$connection, TRUE)
    if (req$url == "https://ok") {
      response(body = StreamingBody$new(rawConnection(charToRaw("a\nb\n"))))
    } else {
      response(404)
    }
  })

  resp <- await_promise(
    req_perform_connection_promise(request("https://ok"))
  )
  expect_equal(resp_stream_lines(resp, 2), c("a", "b"))
  close(resp)

  expect_error(
    await_promise(req_perform_connection_promise(request("https://notok"))),
    class = "httr2_http_404"
  )
})

test_that("can detect the headers of the final response", {
  curl_data <- function(status, headers) {
    list(status_code = status, headers = charToRaw(headers))
  }

  expect_equal(has_final_headers(curl_data(200, "HTTP/1.1 200\r\n\r\n")), TRUE)
  expect_equal(has_final_headers(curl_data(200, "HTTP/1.1 200\n\n")), TRUE)
  expect_equal(has_final_headers(curl_data(404, "HTTP/1.1 404\r\n\r\n")), TRUE)

  expect_equal(has_final_headers(curl_data(0, "")), FALSE)
  expect_equal(has_final_headers(curl_data(200, "HTTP/1.1 200\r\n")), FALSE)
  expect_equal(has_final_headers(curl_data(100, "HTTP/1.1 100\r\n\r\n")), FALSE)
  expect_equal(has_final_headers(curl_data(302, "HTTP/1.1 302\r\n\r\n")), FALSE)
  expect_equal(has_final_headers(curl_data(401, "HTTP/1.1 401\r\n\r\n")), FALSE)
})
