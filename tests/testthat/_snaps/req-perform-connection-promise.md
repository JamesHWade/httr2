# checks its inputs

    Code
      req_perform_connection_promise(1)
    Condition
      Error in `req_perform_connection_promise()`:
      ! `req` must be an HTTP request object, not the number 1.
    Code
      req_perform_connection_promise(req, pool = "INVALID")
    Condition
      Error in `req_perform_connection_promise()`:
      ! `pool` must be a {curl} pool or `NULL`, not the string "INVALID".
    Code
      req_perform_connection_promise(req, verbosity = "INVALID")
    Condition
      Error in `req_perform_connection_promise()`:
      ! `verbosity` must be 0, 1, 2, or 3.

# close() cancels an in-flight transfer

    Code
      resp_stream_raw(resp)
    Condition
      Error in `resp_stream_raw()`:
      ! `resp` has already been closed.

# must supply pool in temporary loop

    Code
      req_perform_connection_promise(request_test())
    Condition
      Error in `req_perform_connection_promise()`:
      ! Must supply `pool` when calling `later::with_temp_loop()`.
      i Do you need `pool = curl::new_pool()`?

