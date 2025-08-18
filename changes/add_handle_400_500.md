new OperationStep for handling HTTP status codes.

* 301 throws a non-recoverable error
* 400, 404, 500 call the error method and could be reattempted
