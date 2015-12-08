package replaydb.exec.spam

import org.scalatest.FlatSpec

class SpamUtilSpec extends FlatSpec {
  "A SpamUtil" should "properly identify e-mail addresses" in {
    import SpamUtil._
    assert(hasEmail("hello @there") === false)
    assert(hasEmail("test@example.com") === true)
    assert(hasEmail("test_email@example.co.uk") === true)
    assert(hasEmail("test+email@example.co.uk") === true)
    assert(hasEmail("hello test-email@example.com") === true)
    assert(hasEmail("test@yahoo.us is an address") === true)
    assert(hasEmail("test@yahoo us is an address") === false)
  }
}
