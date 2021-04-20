/*
 * Copyright IBM Corporation 2016, 2017, 2018
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import NIO
import NIOHTTP1
import Dispatch
import NIOSSL
import SSLService
import LoggerAPI
import NIOWebSocket
import Foundation
import NIOExtras
import NIOConcurrencyHelpers

#if os(Linux)
import Glibc
import CLinuxHelpers
#else
import Darwin
#endif

// MARK: HTTPServer
/**
An HTTP server that listens for connections on a socket.
### Usage Example: ###
````swift
 //Create a server that listens for connections on a specified socket.
 let server = try HTTPServer.listen(on: 0, delegate: delegate)
 ...
 //Stop the server.
 server.stop()
````
*/

#if os(Linux)
    let numberOfCores = Int(linux_sched_getaffinity())
    fileprivate let globalELG = MultiThreadedEventLoopGroup(numberOfThreads: numberOfCores > 0 ? numberOfCores : System.coreCount)
#else
    fileprivate let globalELG = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
#endif

public class HTTPServer: Server {

    public typealias ServerType = HTTPServer

    /**
     HTTP `ServerDelegate`.

     ### Usage Example: ###
     ````swift
     httpServer.delegate = self
     ````
    */
    public var delegate: ServerDelegate?

    /// The TCP port on which this server listens for new connections. If `nil`, this server does not listen on a TCP socket.
    public private(set) var port: Int?

    /// The address of a network interface to listen on, for example "localhost". The default is nil,
    /// which listens for connections on all interfaces.
    public private(set) var address: String?

    /// The Unix domain socket path on which this server listens for new connections. If `nil`, this server does not listen on a Unix socket.
    public private(set) var unixDomainSocketPath: String?

    private var _state: ServerState = .unknown

    private let syncQ = DispatchQueue(label: "HTTPServer.syncQ")

    /**
     A server state

     ### Usage Example: ###
     ````swift
     if(httpSever.state == .unknown) {
        httpServer.stop()
     }
     ````
    */
    public private(set) var state: ServerState {
        get {
            return self.syncQ.sync {
                return self._state
            }
        }
        set {
            self.syncQ.sync {
                self._state = newValue
            }
        }
    }

    fileprivate let lifecycleListener = ServerLifecycleListener()

    /**
     Controls the maximum number of requests per Keep-Alive connection.

     ### Usage Example: ###
     ````swift
     httpServer.keepAliveState = .unlimited
     ````
    */
    public var keepAliveState: KeepAliveState = .unlimited

    /// The channel used to listen for new connections
    var serverChannel: Channel?

    /**
     Whether or not this server allows port reuse (default: disallowed).

     ### Usage Example: ###
     ````swift
     httpServer.allowPortReuse = true
     ````
    */
    public var allowPortReuse = false

    /// Maximum number of pending connections
    private let maxPendingConnections = 100

    // A lazily initialized EventLoopGroup, accessed via `eventLoopGroup`
    private var _eventLoopGroup: EventLoopGroup?

    /// The EventLoopGroup used by this HTTPServer. This property may be assigned
    /// once and once only, by calling `setEventLoopGroup(value:)` before `listen()` is called.
    /// Server runs on `eventLoopGroup` which it is initialized to i.e. when user explicitly provides `eventLoopGroup` for server,
    /// public variable `eventLoopGroup` will  return value stored private variable `_eventLoopGroup` when `ServerBootstrap` is called in `listen()`
    /// making the server run of userdefined EventLoopGroup. If the `setEventLoopGroup(value:)` is not called, `nil` in variable `_eventLoopGroup` forces
    /// Server to run in `globalELG` since value of `eventLoopGroup` in `ServerBootstrap(group: eventLoopGroup)` gets initialzed to value `globalELG`
    /// if `setEventLoopGroup(value:)` is not called before `listen()`
    /// If you are using Kitura-NIO and need to access EventLoopGroup that Kitura uses, you can do so like this:
    ///
    ///         ```swift
    ///         let eventLoopGroup = server.eventLoopGroup
    ///         ```
    ///
    public var eventLoopGroup: EventLoopGroup {
        if let value = self._eventLoopGroup { return value }
        let value = globalELG
        self._eventLoopGroup = value
        return value
    }

    var quiescingHelper: ServerQuiescingHelper?

    /// server configuration
    public var options: ServerOptions = ServerOptions()

    //counter for no of connections
    var connectionCount = NIOAtomic.makeAtomic(value: 0)

    /**
     Creates an HTTP server object.

     ### Usage Example: ###
     ````swift
     let config =HTTPServerConfiguration(requestSize: 1000, coonectionLimit: 100)
     let server = HTTPServer(serverconfig: config)
     server.listen(on: 8080)
     ````
    */
    public init(options: ServerOptions = ServerOptions()) {
        self.options = options
    }

    /**
     SSL cert configuration for handling client requests.

     ### Usage Example: ###
     ````swift
     httpServer.sslConfig = sslConfiguration
     ````
    */
    public var sslConfig: SSLService.Configuration? {
        didSet {
            if let sslConfig = sslConfig {
                //convert to TLSConfiguration
                let config = SSLConfiguration(sslConfig: sslConfig)
                tlsConfig = config.tlsServerConfig()
            }
        }
    }

    /// NIOSSL.TLSConfiguration used with the ServerBootstrap
    private var tlsConfig: TLSConfiguration?

    /// The SSLContext built using the TLSConfiguration
    private var sslContext: NIOSSLContext?

    /// URI for which the latest WebSocket upgrade was requested by a client
    var latestWebSocketURI: String = "/<unknown>"

    /// Determines if the request should be upgraded and adds additional upgrade headers to the request
    private func shouldUpgradeToWebSocket(channel: Channel, webSocketHandlerFactory: ProtocolHandlerFactory, head: HTTPRequestHead) -> EventLoopFuture<HTTPHeaders?> {
        self.latestWebSocketURI = String(head.uri.split(separator: "?")[0])
        guard webSocketHandlerFactory.isServiceRegistered(at: self.latestWebSocketURI) else { return channel.eventLoop.makeSucceededFuture(nil) }
        var headers = HTTPHeaders()
        if let wsProtocol = head.headers["Sec-WebSocket-Protocol"].first {
            headers.add(name: "Sec-WebSocket-Protocol", value: wsProtocol)
        }
        if let key =  head.headers["Sec-WebSocket-Key"].first {
            headers.add(name: "Sec-WebSocket-Key", value: key)
        }
        if let _extension = head.headers["Sec-WebSocket-Extensions"].first {
            let responseExtensions = webSocketHandlerFactory.negotiate(header: _extension)
            // A Safari bug causes the connection to be dropped if an empty header is sent
            if !responseExtensions.isEmpty {
                headers.add(name: "Sec-WebSocket-Extensions", value: responseExtensions)
            }
        }
        return channel.eventLoop.makeSucceededFuture(headers)
    }

    /// Creates upgrade request and adds WebSocket handler to pipeline
    private func upgradeHandler(channel: Channel, webSocketHandlerFactory: ProtocolHandlerFactory, request: HTTPRequestHead) -> EventLoopFuture<Void> {
        return channel.eventLoop.submit {
            let request = HTTPServerRequest(channel: channel, requestHead: request, enableSSL: false)
            return webSocketHandlerFactory.handler(for: request)
            }.flatMap { (handler: ChannelHandler) -> EventLoopFuture<Void> in
                return channel.pipeline.addHandler(handler).flatMap {
                    if let _extensions = request.headers["Sec-WebSocket-Extensions"].first {
                        let handlers = webSocketHandlerFactory.extensionHandlers(header: _extensions)
                        return channel.pipeline.addHandlers(handlers, position: .before(handler))
                    } else {
                        // No extensions. We must return success.
                        return channel.eventLoop.makeSucceededFuture(())
                    }
                }
            }
    }

    private typealias ShouldUpgradeFunction = (Channel, HTTPRequestHead) -> EventLoopFuture<HTTPHeaders?>
    private typealias UpgradePipelineHandlerFunction = (Channel, HTTPRequestHead) -> EventLoopFuture<Void>

    private func generateShouldUpgrade(_ webSocketHandlerFactory: ProtocolHandlerFactory) -> ShouldUpgradeFunction {
        return { (channel: Channel, head: HTTPRequestHead) in
            return self.shouldUpgradeToWebSocket(channel: channel, webSocketHandlerFactory: webSocketHandlerFactory, head: head)
        }
    }

    private func generateUpgradePipelineHandler(_ webSocketHandlerFactory: ProtocolHandlerFactory) -> UpgradePipelineHandlerFunction {
        return { (channel: Channel, request: HTTPRequestHead) in
            return self.upgradeHandler(channel: channel, webSocketHandlerFactory: webSocketHandlerFactory, request: request)
        }
    }

    private func createNIOSSLServerHandler() -> NIOSSLServerHandler? {
        if let sslContext = self.sslContext {
            return NIOSSLServerHandler(context: sslContext)
        }
        return nil
    }

    // Sockets could either be TCP/IP sockets or Unix domain sockets
    private enum SocketType {
        // An TCP/IP socket has an associated port number and optional address value
        case tcp(Int, String?)
        // A unix domain socket has an associated filename
        case unix(String)
    }

    /**
     Listens for connections on a Unix socket.

     ### Usage Example: ###
     ````swift
     try server.listen(unixDomainSocketPath: "/my/path")
     ````

     - Parameter unixDomainSocketPath: Unix socket path for new connections, eg. "/my/path"
     */
    public func listen(unixDomainSocketPath: String) throws {
        self.unixDomainSocketPath = unixDomainSocketPath
        try listen(.unix(unixDomainSocketPath))
    }

    /**
     Listens for connections on a TCP socket.

     ### Usage Example: ###
     ````swift
     try server.listen(on: 8080, address: "localhost")
     ````

     - Parameter on: Port number for new connections, e.g. 8080
     - Parameter address: The address of the network interface to listen on. Defaults to nil, which means this server
                          will listen on all interfaces.
     */
    public func listen(on port: Int, address: String? = nil) throws {
        self.port = port
        self.address = address
        try listen(.tcp(port, address))
    }

    /// Sets the EventLoopGroup to be used by this HTTPServer. This may be called once
    /// and once only, and must be called prior to `listen()`.
    /// - Throws: If the EventLoopGroup has already been assigned.
    /// If you are using Kitura-NIO and need to set EventLoopGroup that Kitura uses, you can do so like this:
    ///
    ///         ```swift
    ///         server.setEventLoopGroup(EventLoopGroup)
    ///         ```
    ///
    /// - Parameter : this function is supplied with user defined EventLoopGroup as arguement
    public func setEventLoopGroup(_ value: EventLoopGroup) throws {
        guard _eventLoopGroup == nil else {
            throw HTTPServerError.eventLoopGroupAlreadyInitialized
        }
        _eventLoopGroup = value
    }

    private func listen(_ socket: SocketType) throws {

        if let tlsConfig = tlsConfig {
            do {
                self.sslContext = try NIOSSLContext(configuration: tlsConfig)
            } catch let error {
                Log.error("Failed to create SSLContext. Error: \(error)")
            }
        }

        var upgraders: [HTTPServerProtocolUpgrader] = []
        if let webSocketHandlerFactory = ConnectionUpgrader.getProtocolHandlerFactory(for: "websocket") {
            ///TODO: Should `maxFrameSize` be configurable?
            let upgrader = KituraWebSocketUpgrader(maxFrameSize: 1 << 24,
                                                   automaticErrorHandling: false,
                                                   shouldUpgrade: generateShouldUpgrade(webSocketHandlerFactory),
                                                   upgradePipelineHandler: generateUpgradePipelineHandler(webSocketHandlerFactory))
            upgraders.append(upgrader)
        }

        let bootstrap = ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.backlog, value: .init(self.maxPendingConnections))
            .serverChannelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .serverChannelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEPORT), value: allowPortReuse ? 1 : 0)
            .serverChannelInitializer { channel in
                // Adding the quiescing helper will help us do a graceful stop()
                self.quiescingHelper = ServerQuiescingHelper(group: self.eventLoopGroup)
                return channel.pipeline.addHandler(self.quiescingHelper!.makeServerChannelHandler(channel: channel))
            }
            .childChannelInitializer { channel in
                let httpHandler = HTTPRequestHandler(for: self)
                let config: NIOHTTPServerUpgradeConfiguration = (upgraders: upgraders, completionHandler: {_ in 
                    _ = channel.pipeline.removeHandler(httpHandler)
                })
                return channel.pipeline.configureHTTPServerPipeline(withServerUpgrade: config, withErrorHandling: true).flatMap {
                    if let nioSSLServerHandler = self.createNIOSSLServerHandler() {
                        _ = channel.pipeline.addHandler(nioSSLServerHandler, position: .first)
                    }
                    return channel.pipeline.addHandler(httpHandler)
                }
            }

        let listenerDescription: String
        do {
            switch socket {
            case SocketType.tcp(let port, let address):
                serverChannel = try bootstrap.bind(host: address ?? "0.0.0.0", port: port).wait()
                self.port = serverChannel?.localAddress?.port.map { Int($0) }
                listenerDescription = "port \(self.port ?? port)"
            case SocketType.unix(let unixDomainSocketPath):
                // Ensure the path doesn't exist...
                #if os(Linux)
                _ = Glibc.unlink(unixDomainSocketPath)
                #else
                _ = Darwin.unlink(unixDomainSocketPath)
                #endif
                serverChannel = try bootstrap.bind(unixDomainSocketPath: unixDomainSocketPath).wait()
                self.unixDomainSocketPath = unixDomainSocketPath
                listenerDescription = "path \(unixDomainSocketPath)"
            }
            self.state = .started
            self.lifecycleListener.performStartCallbacks()
        } catch let error {
            self.state = .failed
            self.lifecycleListener.performFailCallbacks(with: error)
            switch socket {
            case .tcp(let port, let address):
                Log.error("Error trying to bind to \(address ?? "")\(port): \(error)")
            case .unix(let socketPath):
                Log.error("Error trying to bind to \(socketPath): \(error)")
            }
            throw error
        }

        Log.info("Listening on \(listenerDescription)")
        Log.verbose("Options for \(listenerDescription): maxPendingConnections: \(maxPendingConnections), allowPortReuse: \(self.allowPortReuse)")

        let queuedBlock = DispatchWorkItem(block: {
            guard let serverChannel = self.serverChannel else { return }
            do {
                try serverChannel.closeFuture.wait()
            } catch let error {
                Log.error("Error while closing channel: \(error)")
            }
            self.state = .stopped
            self.lifecycleListener.performStopCallbacks()
        })
        ListenerGroup.enqueueAsynchronously(on: DispatchQueue.global(), block: queuedBlock)
    }

    /**
     Static method to create a new HTTP server and have it listen for connections.

     ### Usage Example: ###
     ````swift
     let server = HTTPServer.listen(on: 8080, node: "localhost", delegate: self)
     ````

     - Parameter on: Port number for accepting new connections.
     - Parameter address: The address of the network interface to listen on. Defaults to nil, which means this server
                 will listen on all interfaces.
     - Parameter delegate: The delegate handler for HTTP connections.

     - Returns: A new instance of a `HTTPServer`.
    */
    public static func listen(on port: Int, address: String? = nil, delegate: ServerDelegate?) throws -> ServerType {
        let server = HTTP.createServer()
        server.delegate = delegate
        try server.listen(on: port, address: address)
        return server
    }

    /**
     Static method to create a new HTTP server and have it listen for connections on a Unix domain socket.

     ### Usage Example: ###
     ````swift
     let server = HTTPServer.listen(unixDomainSocketPath: "/my/path", delegate: self)
     ````

     - Parameter unixDomainSocketPath: The path of the Unix domain socket that this server should listen on.
     - Parameter delegate: The delegate handler for HTTP connections.

     - Returns: A new instance of a `HTTPServer`.
     */
    public static func listen(unixDomainSocketPath: String, delegate: ServerDelegate?) throws -> HTTPServer {
        let server = HTTP.createServer()
        server.delegate = delegate
        try server.listen(unixDomainSocketPath: unixDomainSocketPath)
        return server
    }

    /**
     Listen for connections on a socket.

     ### Usage Example: ###
     ````swift
     try server.listen(on: 8080, errorHandler: errorHandler)
     ````
     - Parameter port: port number for new connections (eg. 8080)
     - Parameter errorHandler: optional callback for error handling
    */
    @available(*, deprecated, message: "use 'listen(on:) throws' with 'server.failed(callback:)' instead")
    public func listen(port: Int, errorHandler: ((Swift.Error) -> Void)?) {
        do {
            try listen(on: port)
        } catch let error {
            if let callback = errorHandler {
                callback(error)
            } else {
                Log.error("Error listening on port \(port): \(error)")
            }
        }
    }

    /**
     Static method to create a new HTTPServer and have it listen for connections.

     ### Usage Example: ###
     ````swift
     let server = HTTPServer(port: 8080, delegate: self, errorHandler: errorHandler)
     ````
     - Parameter port: port number for new connections (eg. 8080)
     - Parameter delegate: The delegate handler for HTTP connections.
     - Parameter errorHandler: optional callback for error handling

     - Returns: A new `HTTPServer` instance.
    */
    @available(*, deprecated, message: "use 'listen(on:delegate:) throws' with 'server.failed(callback:)' instead")
    public static func listen(port: Int, delegate: ServerDelegate, errorHandler: ((Swift.Error) -> Void)?) -> ServerType {
        let server = HTTP.createServer()
        server.delegate = delegate
        server.listen(port: port, errorHandler: errorHandler)
        return server
    }

    /**
     Stop listening for new connections.

     ### Usage Example: ###
     ````swift
     server.stop()
     ````
    */
    public func stop() {
        // Close the listening channel
        guard let serverChannel = serverChannel else { return }
        do {
            try serverChannel.close().wait()
        } catch let error {
            Log.error("Failed to close the server channel. Error: \(error)")
        }

        // Now close all the open channels
        guard let quiescingHelper = self.quiescingHelper else { return }
        let fullShutdownPromise: EventLoopPromise<Void> = eventLoopGroup.next().makePromise()
        quiescingHelper.initiateShutdown(promise: fullShutdownPromise)
        fullShutdownPromise.futureResult.whenComplete { _ in
            self.state = .stopped
        }
    }

    /**
     Add a new listener for a server being started.

     ### Usage Example: ###
     ````swift
     server.started(callback: callBack)
     ````
     - Parameter callback: The listener callback that will run after a successfull start-up.

     - Returns: A `HTTPServer` instance.
    */
    @discardableResult
    public func started(callback: @escaping () -> Void) -> Self {
        self.lifecycleListener.addStartCallback(perform: self.state == .started, callback)
        return self
    }

    /**
     Add a new listener for a server being stopped.

     ### Usage Example: ###
     ````swift
     server.stopped(callback: callBack)
     ````
     - Parameter callback: The listener callback that will run when the server stops.

     - Returns: A `HTTPServer` instance.
    */
    @discardableResult
    public func stopped(callback: @escaping () -> Void) -> Self {
        self.lifecycleListener.addStopCallback(perform: self.state == .stopped, callback)
        return self
    }

    /**
     Add a new listener for a server throwing an error.

     ### Usage Example: ###
     ````swift
     server.started(callback: callBack)
     ````
     - Parameter callback: The listener callback that will run when the server throws an error.

     - Returns: A `HTTPServer` instance.
    */
    @discardableResult
    public func failed(callback: @escaping (Swift.Error) -> Void) -> Self {
        self.lifecycleListener.addFailCallback(callback)
        return self
    }
    /**
     Add a new listener for when `listenSocket.acceptClientConnection` throws an error.

     ### Usage Example: ###
     ````swift
     server.clientConnectionFailed(callback: callBack)
     ````
     - Parameter callback: The listener callback that will run on server after successfull start-up.

     - Returns: A `HTTPServer` instance.
    */
    @discardableResult
    public func clientConnectionFailed(callback: @escaping (Swift.Error) -> Void) -> Self {
        self.lifecycleListener.addClientConnectionFailCallback(callback)
        return self
    }
}

class HTTPDummyServerDelegate: ServerDelegate {
    /// Handle new incoming requests to the server
    ///
    /// - Parameter request: The ServerRequest class instance for working with this request.
    ///                     The ServerRequest object enables you to get the query parameters, headers, and body amongst other
    ///                     information about the incoming request.
    /// - Parameter response: The ServerResponse class instance for working with this request.
    ///                     The ServerResponse object enables you to build and send your response to the client who sent
    ///                     the request. This includes headers, the body, and the response code.
    func handle(request: ServerRequest, response: ServerResponse) {
        do {
            response.statusCode = .notFound
            let theBody = "Path not found"
            response.headers["Content-Type"] = ["text/plain"]
            response.headers["Content-Length"] = [String(theBody.utf8.count)]
            try response.write(from: theBody)
            try response.end()
        } catch {
            Log.error("Failed to send the response. Error = \(error)")
        }
    }
}

// A specialized Websocket upgrader for Kitura.

// A WebSocket upgrade request is accompanied with two mandatory headers - `Sec-WebSocket-Version` and `Sec-WebSocket-Key`.
// An upgrade fails if either of these headers are absent or if they have multiple values.
// An upgrade also fails if the `Sec-WebSocket-Version` value is not set to 13.
// This means we have at least three kinds of errors related to invalid upgrade headers. However, the `NIOWebSocket.NIOWebSocketUpgradeError`
// type does not have cases that indicate all these three types of errors. There's only a general `invalidUpgradeHeader` case.
// A WebSocket server that catches this error has no way to inform the client about what exactly went wrong.
//
// Issue created with swift-nio: https://github.com/apple/swift-nio/issues/577
//
// A work-around suggested in the issue is to create a wrapper-upgrader around the existing upgrader and generate the more granular errors therein.
// This work-around will have to be removed once the limitation is removed from swift-nio, possibly in version 2.0

// TODO: Re-evaluate the need for this class when swift-nio 2.0 is released.
final class KituraWebSocketUpgrader: HTTPServerProtocolUpgrader {
    private let _wrappedUpgrader: NIOWebSocketServerUpgrader

    public init(maxFrameSize: Int, automaticErrorHandling: Bool = true,
                shouldUpgrade: @escaping (Channel, HTTPRequestHead) -> EventLoopFuture<HTTPHeaders?>,
                upgradePipelineHandler: @escaping (Channel, HTTPRequestHead) -> EventLoopFuture<Void>) {
        _wrappedUpgrader = NIOWebSocketServerUpgrader(maxFrameSize: maxFrameSize, automaticErrorHandling: automaticErrorHandling, shouldUpgrade: shouldUpgrade,
                                             upgradePipelineHandler: upgradePipelineHandler)
    }

    public convenience init(automaticErrorHandling: Bool = true, shouldUpgrade: @escaping (Channel, HTTPRequestHead) -> EventLoopFuture<HTTPHeaders?>,
                            upgradePipelineHandler: @escaping (Channel, HTTPRequestHead) -> EventLoopFuture<Void>) {
        self.init(maxFrameSize: 1 << 14, automaticErrorHandling: automaticErrorHandling,
                  shouldUpgrade: shouldUpgrade, upgradePipelineHandler: upgradePipelineHandler)
    }

    public var supportedProtocol: String {
        return self._wrappedUpgrader.supportedProtocol
    }

    public var requiredUpgradeHeaders: [String] {
        return _wrappedUpgrader.requiredUpgradeHeaders
    }

    public func buildUpgradeResponse(channel: Channel, upgradeRequest: HTTPRequestHead, initialResponseHeaders: HTTPHeaders) -> EventLoopFuture<HTTPHeaders> {
        let future = _wrappedUpgrader.buildUpgradeResponse(channel: channel, upgradeRequest: upgradeRequest, initialResponseHeaders: initialResponseHeaders)
        return future.flatMapError { error in
            guard let upgradeError = error as? NIOWebSocketUpgradeError, upgradeError == NIOWebSocketUpgradeError.invalidUpgradeHeader else {
                return channel.eventLoop.makeFailedFuture(error)
            }

            let keyHeader = upgradeRequest.headers[canonicalForm: "Sec-WebSocket-Key"]
            let versionHeader = upgradeRequest.headers[canonicalForm: "Sec-WebSocket-Version"]

            var error: KituraWebSocketUpgradeError 
            if keyHeader.count == 0 {
                error = KituraWebSocketUpgradeError.noWebSocketKeyHeader
            } else if keyHeader.count > 1 {
                error = KituraWebSocketUpgradeError.invalidKeyHeaderCount(keyHeader.count)
            } else if versionHeader.count == 0 {
                error = KituraWebSocketUpgradeError.noWebSocketVersionHeader
            } else if versionHeader.count > 1 {
                error = KituraWebSocketUpgradeError.invalidVersionHeaderCount(versionHeader.count)
            } else if versionHeader.first! != "13" {
                error = KituraWebSocketUpgradeError.invalidVersionHeader(String(versionHeader.first!))
            } else {
                error = KituraWebSocketUpgradeError.unknownUpgradeError 
            }
            return channel.eventLoop.makeFailedFuture(error)
        }.flatMap { value in 
            return channel.eventLoop.makeSucceededFuture(value)
        }
    }

    public func upgrade(context: ChannelHandlerContext, upgradeRequest: HTTPRequestHead) -> EventLoopFuture<Void> {
        return _wrappedUpgrader.upgrade(context: context, upgradeRequest: upgradeRequest)
    }
}

// Detailed WebSocket upgrade errors
// TODO: Re-evaluate the need for this enum after swift-nio 2.0 is released.
enum KituraWebSocketUpgradeError: Error {
    // The upgrade request had no Sec-WebSocket-Key header
    case noWebSocketKeyHeader

    // The upgrade request had no Sec-WebSocket-Version header
    case noWebSocketVersionHeader

    // The upgrade request had multiple Sec-WebSocket-Key header values
    case invalidKeyHeaderCount(Int)

    // The upgrade request had multiple Sec-WebSocket-Version header values
    case invalidVersionHeaderCount(Int)

    // The Sec-WebSocket-Version is not 13
    case invalidVersionHeader(String)

    // Unknown upgrade error
    case unknownUpgradeError
}

/// Errors thrown by HTTPServer
public struct HTTPServerError: Error, Equatable {

    internal enum HTTPServerErrorType: Error {
        case eventLoopGroupAlreadyInitialized
    }

    private var _httpServerError: HTTPServerErrorType

    private init(value: HTTPServerErrorType){
        self._httpServerError = value
    }

    public static var eventLoopGroupAlreadyInitialized = HTTPServerError(value: .eventLoopGroupAlreadyInitialized)
}
