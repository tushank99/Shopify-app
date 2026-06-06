const asyncHandler = (fn) => (req, res, next) => {
  // Forward any error to Express's error-handling middleware (errorHandler).
  // Do NOT send a response here: doing so hardcodes 500 and can throw
  // ERR_HTTP_HEADERS_SENT (crashing the process) if a response was already sent.
  Promise.resolve(fn(req, res, next)).catch(next);
};

export default asyncHandler;
