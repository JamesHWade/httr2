test_that("read() returns bytes across chunk boundaries", {
  body <- MultiStreamingBody$new(curl::new_handle())
  body$push(as.raw(1:3))
  body$push(as.raw(4:6))
  body$finish()

  expect_equal(body$is_complete(), FALSE)
  expect_equal(body$read(2), as.raw(1:2))
  expect_equal(body$read(3), as.raw(3:5))
  expect_equal(body$read(10), as.raw(6))
  expect_equal(body$read(10), raw())
  expect_equal(body$is_complete(), TRUE)
})

test_that("read_all() returns everything", {
  body <- MultiStreamingBody$new(curl::new_handle())
  for (i in 1:100) {
    body$push(as.raw(i))
  }
  body$finish()
  expect_equal(body$read_all(buffer = 7), as.raw(1:100))
  expect_equal(body$is_open(), FALSE)
})

test_that("buffered bytes can be read before a transfer error is raised", {
  body <- MultiStreamingBody$new(curl::new_handle())
  body$push(charToRaw("partial"))
  body$finish(error_cnd("httr2_failure", message = "Transfer failed"))

  expect_equal(body$read(10), charToRaw("partial"))
  expect_equal(body$is_complete(), FALSE)
  expect_error(body$read(10), class = "httr2_failure")
  expect_equal(body$is_complete(), TRUE)
})

test_that("closing the body cancels its pooled handle once", {
  pool <- curl::new_pool()
  handle <- curl::new_handle()
  curl::multi_add(handle, pool = pool)
  cancellations <- 0L
  body <- MultiStreamingBody$new(handle, pool, function() {
    cancellations <<- cancellations + 1L
  })

  expect_length(curl::multi_list(pool), 1)
  body$close()
  body$close()
  expect_length(curl::multi_list(pool), 0)
  expect_equal(cancellations, 1L)
  expect_equal(body$is_open(), FALSE)
})
