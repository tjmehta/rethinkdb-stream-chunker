// Generated by CoffeeScript 1.10.0
var convertPseudotype, err, errorClass, mkAtom, mkErr, mkSeq, plural, protoErrorType, protodef, recursivelyConvertPseudotype,
  slice = [].slice;

err = require('./errors');

protodef = require('./proto-def');

protoErrorType = protodef.Response.ErrorType;

plural = function(number) {
  if (number === 1) {
    return "";
  } else {
    return "s";
  }
};

module.exports.ar = function(fun) {
  return function() {
    var args;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    if (args.length !== fun.length) {
      throw new err.ReqlDriverCompileError("Expected " + fun.length + " argument" + (plural(fun.length)) + " but found " + args.length + ".");
    }
    return fun.apply(this, args);
  };
};

module.exports.varar = function(min, max, fun) {
  return function() {
    var args;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    if (((min != null) && args.length < min) || ((max != null) && args.length > max)) {
      if ((min != null) && (max == null)) {
        throw new err.ReqlDriverCompileError("Expected " + min + " or more arguments but found " + args.length + ".");
      }
      if ((max != null) && (min == null)) {
        throw new err.ReqlDriverCompileError("Expected " + max + " or fewer arguments but found " + args.length + ".");
      }
      throw new err.ReqlDriverCompileError("Expected between " + min + " and " + max + " arguments but found " + args.length + ".");
    }
    return fun.apply(this, args);
  };
};

module.exports.aropt = function(fun) {
  return function() {
    var args, expectedPosArgs, numPosArgs, perhapsOptDict;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    expectedPosArgs = fun.length - 1;
    perhapsOptDict = args[expectedPosArgs];
    if ((perhapsOptDict != null) && (Object.prototype.toString.call(perhapsOptDict) !== '[object Object]')) {
      perhapsOptDict = null;
    }
    numPosArgs = args.length - (perhapsOptDict != null ? 1 : 0);
    if (expectedPosArgs !== numPosArgs) {
      if (expectedPosArgs !== 1) {
        throw new err.ReqlDriverCompileError("Expected " + expectedPosArgs + " arguments (not including options) but found " + numPosArgs + ".");
      } else {
        throw new err.ReqlDriverCompileError("Expected " + expectedPosArgs + " argument (not including options) but found " + numPosArgs + ".");
      }
    }
    return fun.apply(this, args);
  };
};

module.exports.toArrayBuffer = function(node_buffer) {
  var arr, i, j, len, value;
  arr = new Uint8Array(new ArrayBuffer(node_buffer.length));
  for (i = j = 0, len = node_buffer.length; j < len; i = ++j) {
    value = node_buffer[i];
    arr[i] = value;
  }
  return arr.buffer;
};

module.exports.fromCamelCase = function(token) {
  return token.replace(/[A-Z]/g, (function(_this) {
    return function(match) {
      return "_" + match.toLowerCase();
    };
  })(this));
};

module.exports.toCamelCase = function(token) {
  return token.replace(/_[a-z]/g, (function(_this) {
    return function(match) {
      return match[1].toUpperCase();
    };
  })(this));
};

convertPseudotype = function(obj, opts) {
  var i, j, len, ref, results;
  switch (obj['$reql_type$']) {
    case 'TIME':
      switch (opts.timeFormat) {
        case 'native':
        case void 0:
          if (obj['epoch_time'] == null) {
            throw new err.ReqlDriverError("pseudo-type TIME " + obj + " object missing expected field 'epoch_time'.");
          }
          return new Date(obj['epoch_time'] * 1000);
        case 'raw':
          return obj;
        default:
          throw new err.ReqlDriverError("Unknown timeFormat run option " + opts.timeFormat + ".");
      }
      break;
    case 'GROUPED_DATA':
      switch (opts.groupFormat) {
        case 'native':
        case void 0:
          ref = obj['data'];
          results = [];
          for (j = 0, len = ref.length; j < len; j++) {
            i = ref[j];
            results.push({
              group: i[0],
              reduction: i[1]
            });
          }
          return results;
          break;
        case 'raw':
          return obj;
        default:
          throw new err.ReqlDriverError("Unknown groupFormat run option " + opts.groupFormat + ".");
      }
      break;
    case 'BINARY':
      switch (opts.binaryFormat) {
        case 'native':
        case void 0:
          if (obj['data'] == null) {
            throw new err.ReqlDriverError("pseudo-type BINARY object missing expected field 'data'.");
          }
          return new Buffer(obj['data'], 'base64');
        case 'raw':
          return obj;
        default:
          throw new err.ReqlDriverError("Unknown binaryFormat run option " + opts.binaryFormat + ".");
      }
      break;
    default:
      return obj;
  }
};

recursivelyConvertPseudotype = function(obj, opts) {
  var i, j, key, len, value;
  if (Array.isArray(obj)) {
    for (i = j = 0, len = obj.length; j < len; i = ++j) {
      value = obj[i];
      obj[i] = recursivelyConvertPseudotype(value, opts);
    }
  } else if (obj && typeof obj === 'object') {
    for (key in obj) {
      value = obj[key];
      obj[key] = recursivelyConvertPseudotype(value, opts);
    }
    obj = convertPseudotype(obj, opts);
  }
  return obj;
};

mkAtom = function(response, opts) {
  return recursivelyConvertPseudotype(response.r[0], opts);
};

mkSeq = function(response, opts) {
  return recursivelyConvertPseudotype(response.r, opts);
};

mkErr = function(ErrClass, response, root) {
  return new ErrClass(mkAtom(response), root, response.b);
};

errorClass = (function(_this) {
  return function(errorType) {
    switch (errorType) {
      case protoErrorType.QUERY_LOGIC:
        return err.ReqlQueryLogicError;
      case protoErrorType.NON_EXISTENCE:
        return err.ReqlNonExistenceError;
      case protoErrorType.RESOURCE_LIMIT:
        return err.ReqlResourceLimitError;
      case protoErrorType.USER:
        return err.ReqlUserError;
      case protoErrorType.INTERNAL:
        return err.ReqlInternalError;
      case protoErrorType.OP_FAILED:
        return err.ReqlOpFailedError;
      case protoErrorType.OP_INDETERMINATE:
        return err.ReqlOpIndeterminateError;
      default:
        return err.ReqlRuntimeError;
    }
  };
})(this);

module.exports.recursivelyConvertPseudotype = recursivelyConvertPseudotype;

module.exports.mkAtom = mkAtom;

module.exports.mkSeq = mkSeq;

module.exports.mkErr = mkErr;

module.exports.errorClass = errorClass;
