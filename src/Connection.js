import colors from 'colors/safe';
import {errToString} from './util';
import moment from 'moment';
import net from 'net';
import tls from 'tls';
import protodef from 'rethinkdb/proto-def';
import url from 'url';

// A Connection instance wraps an incoming WebSocket (likely from a browser
// using rethinkdb-websocket-client) and an outgoing RethinkDB TCP socket. Once
// start() is called, it will forward traffic in both directions until either
// side disconnects. Each incoming WebSocket spawns a new outgoing TCP socket.
export class Connection {
  constructor(queryValidator, webSocket, loggingMode) {
    this.queryValidator = queryValidator;
    this.webSocket = webSocket;
    this.loggingMode = loggingMode;
    this.remoteAddress = webSocket._socket.remoteAddress;
    this.remotePort = webSocket._socket.remotePort;
  }

  start({sessionCreator, dbHost, dbPort, dbAuthKey, dbSsl}) {
    const urlQueryParams = url.parse(this.webSocket.upgradeReq.url, true).query;
    const req = this.webSocket.upgradeReq;
    this.sessionPromise = sessionCreator(urlQueryParams, req).catch(e => {
      this.sendWebSocketMessage('rethinkdb-websocket-server session rejected\0');
      this.cleanupAndLogErr('Error in sessionCreator', e);
    });
    this.dbAuthKey = dbAuthKey;
    this.wsInBuffer = new Buffer(0);
    this.handshakeComplete = false;
    this.isClosed = false;
    let options = {
      host: dbHost,
      port: dbPort
    };
    if (typeof dbSsl === 'boolean' && dbSsl) {
      this.dbSocket = tls.connect(options);
    } else if (typeof dbSsl === 'object') {
      options = {...dbSsl, ...options};
      this.dbSocket = tls.connect(options);
    } else {
      this.dbSocket = net.connect(options);
    }
    this.dbAuthKey = dbAuthKey;
    this.setupDbSocket();
    this.setupWebSocket();
    if (this.loggingMode === 'all') {
      this.log('Connect');
    }
  }

  sendWebSocketMessage(data) {
    if (this.webSocket.protocol === 'base64') {
      const b64EncodedData = (new Buffer(data)).toString('base64');
      this.webSocket.send(b64EncodedData);
    } else {
      this.webSocket.send(data, {binary: true});
    }
  }

  setupDbSocket() {
    this.dbSocket.setNoDelay();
    this.dbSocket.on('data', data => {
      if (!this.isClosed) {
        try {
          this.sendWebSocketMessage(data);
        } catch (e) {
          this.cleanupAndLogErr('Error recv dbSocket data', e);
        }
      }
    });
    this.dbSocket.on('end', () => {
      this.cleanup();
    });
    this.dbSocket.on('close', () => {
      if (this.loggingMode === 'all') {
        this.log('dbSocket closed');
      }
      this.cleanup();
    });
    this.dbSocket.on('error', e => {
      this.cleanupAndLogErr('dbSocket error', e);
    });
  }

  setupWebSocket() {
    this.webSocket.on('message', msg => {
      if (!this.isClosed) {
        try {
          this.handleWsMessage(msg);
        } catch (e) {
          this.cleanupAndLogErr('Error recv webSocket data', e);
        }
      }
    });
    this.webSocket.on('close', () => {
      if (this.loggingMode === 'all') {
        this.log('webSocket closed');
      }
      this.cleanup();
    });
    this.webSocket.on('error', e => {
      this.cleanupAndLogErr('webSocket error', e);
    });
  }

  log(msg, token) {
    const time = colors.blue(moment().format('HH:mm:ss.SSS'));
    const addr = colors.gray(this.remoteAddress + ':' + this.remotePort);
    const tokenStr = token ? colors.yellow(`tkn=${token} `) : '';
    console.log(`${time} ${addr} ${tokenStr}${msg}`);
  }

  cleanup() {
    this.isClosed = true;
    this.dbSocket.destroy();
    this.webSocket.close();
  }

  cleanupAndLogErr(msg, error) {
    this.cleanup();
    const fullMsg = error ? msg + '\n' + errToString(error) : msg;
    this.log(colors.red(fullMsg));
  }

  processClientCommand(cmdBuf) {
    // https://github.com/rethinkdb/rethinkdb/blob/61692c0ed4/drivers/javascript/net.coffee#L269-L273
    const token = cmdBuf.readUInt32LE(0) + 0x100000000 * cmdBuf.readUInt32LE(4);
    const queryCmdBuf = cmdBuf.slice(12, cmdBuf.length);
    const logFn = this.log.bind(this);
    // Previous code that is being replaced Start
    // const validatePromise = this.sessionPromise.then(session => (
    //   this.queryValidator.validateQueryCmd(token, queryCmdBuf, session, logFn)
    // ));
    // Previous code that is being replaced End
    // Modify the query before sending to RethinkDB Server
    // This modification will allow us to use sessionId in string or integer form
    // sessionId / sessionIdInt will need to be set while creating session
    // using options.sessionCreator.
    // see the following sessionCreator code
    // options.sessionCreator = function (urlParams) {
    //   if (!urlParams.token) {
    //     return Promise.reject('Invalid auth token');
    //   } else {
    //     // your custom authentication code that will
    //     // fetch the sessionId using the token
    //     // this sessionId or sessionIdInt will be then used in the queries
    //     // replacing "$sessionId$" and "$sessionIdInt$" respectively
    //     return Promise.resolve({
    //       sessionId: "a string session id", // optional
    //       sessionIdInt: parseInt("an integer session id"), // optional
    //     })
    //   }
    // };
    const _this = this;
    const validatePromise = this.sessionPromise.then(function(session) {
      // decode the query
      let rawQuery = queryCmdBuf.toString('utf8');
      // find if the query requires $sessionId$ replacement
      // and if session.sessionId exists
      if (rawQuery.indexOf("$sessionId$") > 0 && session.sessionId) {
        // replace the $sessionId with session.sessionId
        rawQuery = rawQuery.replace("$sessionId$", session.sessionId);
      }

      // find if the query requires $sessionIdInt$ replacement
      // and if session.sessionIdInt exists
      if (rawQuery.indexOf("$sessionIdInt$") > 0 && session.sessionIdInt) {
        // replace the $sessionId with session.sessionIdInt
        rawQuery = rawQuery.replace("\"$sessionIdInt$\"", parseInt(session.sessionIdInt));
      }
      // re-convert query to Binary Buffer
      let modifiedQueryCmdBuf = new Buffer(rawQuery.toString('binary'));
      //forward the query for execution

      return _this.queryValidator.validateQueryCmd(token, modifiedQueryCmdBuf, session, logFn);
    });
    return validatePromise.then(allow => {
      if (allow) {
        this.dbSocket.write(cmdBuf, 'binary');
      }
      // TODO It might be nice to send a response back to the client of type
      // CLIENT_ERROR. However, we'd need to parse every message back from
      // the db in order to avoid injecting our response in the middle of a
      // large response.
      //
      // Right now, the query will be silently ignored in the frontend and
      // logged in the backend. This might lead to some sort of memory leak
      // in the frontend if it's waiting for responses from queries. This is
      // not very important, because properly written frontends should avoid
      // submitting queries that are denied.
      //
      // Alternatively, we could close the connection after a denied query,
      // as an easier to debug fail-fast option.
    });
  }

  validateClientHandshake(buf) {
    const protocolVersion = buf.readUInt32LE(0);
    if (protocolVersion !== protodef.VersionDummy.Version.V0_4) {
      this.cleanupAndLogErr('Invalid protocolVersion ' + protocolVersion);
      return 0;
    }
    const keyLength = buf.readUInt32LE(4);
    if (keyLength !== 0) {
      this.cleanupAndLogErr('Auth key not supported');
      return 0;
    }
    const protocolType = buf.readUInt32LE(8);
    if (protocolType !== protodef.VersionDummy.Protocol.JSON) {
      this.cleanupAndLogErr('Protocol type not supported ' + protocolType);
      return 0;
    }
    return 12;
  }

  processNextMessage(buf) {
    if (!this.handshakeComplete) {
      if (buf.length >= 12) {
        const clientHandshakeLength = this.validateClientHandshake(buf);
        if (clientHandshakeLength > 0) {
          const authKey = this.dbAuthKey || '';
          const outBuf = new Buffer(12 + authKey.length);
          outBuf.writeUInt32LE(protodef.VersionDummy.Version.V0_4, 0);
          outBuf.writeUInt32LE(authKey.length, 4);
          outBuf.write(authKey, 8);
          outBuf.writeUInt32LE(protodef.VersionDummy.Protocol.JSON, 8 + authKey.length);
          this.dbSocket.write(outBuf, 'binary');
          this.handshakeComplete = true;
          return clientHandshakeLength;
        } else {
          return 0;
        }
      }
    } else {
      if (buf.length >= 12) {
        const encodedQueryLength = buf.readUInt32LE(8);
        const queryEndOffset = 12 + encodedQueryLength;
        if (queryEndOffset <= buf.length) {
          const cmdBuf = buf.slice(0, queryEndOffset);
          this.processClientCommand(cmdBuf).catch(e => {
            this.cleanupAndLogErr('Error in processClientCommand', e);
          });
          return queryEndOffset;
        }
      }
    }
    return 0;
  }

  handleWsMessage(msg) {
    const isBase64 = typeof msg === 'string' && this.webSocket.protocol === 'base64';
    const incomingBuffer = isBase64 ? new Buffer(msg, 'base64') : msg;
    this.wsInBuffer = Buffer.concat([this.wsInBuffer, incomingBuffer]);
    let keepGoing = true;
    while (keepGoing) {
      let bytesConsumed = this.processNextMessage(this.wsInBuffer);
      this.wsInBuffer = this.wsInBuffer.slice(bytesConsumed);
      keepGoing = bytesConsumed > 0;
    }
  }
}
