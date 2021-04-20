/**
 * Copyright IBM Corporation 2017, 2018
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
 **/

import XCTest

@testable import KituraNet
import Foundation
import NIO
import NIOHTTP1
import NIOSSL
import LoggerAPI
#if os(Linux)
import CLinuxHelpers
#endif

class RegressionTests: KituraNetTest {

    static var allTests: [(String, (RegressionTests) -> () throws -> Void)] {
        return [
            ("testIssue1143", testIssue1143),
            ("testServersCollidingOnPort", testServersCollidingOnPort),
            ("testServersSharingPort", testServersSharingPort),
            ("testBadRequest", testBadRequest),
            ("testBadRequestFollowingGoodRequest", testBadRequestFollowingGoodRequest),
            ("testCustomEventLoopGroup", testCustomEventLoopGroup),
            ("testFailEventLoopGroupReinitialization", testFailEventLoopGroupReinitialization),
        ]
    }

    /// Tests the resolution of Kitura issue 1143: SSL socket listener becomes blocked and
    /// does not accept further connections if a 'bad' connection is made that then sends
    /// no data (where the server is waiting on SSL_accept to receive a handshake).
    ///
    /// The sequence of steps that cause a hang:
    ///
    /// - A non-SSL client connects to SSL listener port, then does nothing
    /// - On the server side, the listener thread expects an SSL client to be connecting.
    ///   It invokes the SSL delegate, goes into SSL_accept and then blocks, waiting for 
    ///   an SSL handshake (which will never arrive)
    /// - Another (well-behaved) SSL client attempts to connect. This hangs, because the
    ///   thread that normally loops around accepting incoming connections is still blocked
    ///   trying to SSL_accept the previous connection.
    ///
    /// The fix for this issue is to decouple the socket accept from the SSL handshake, and
    /// perform the latter on a separate thread. The expected behaviour is that a 'bad'
    /// (non-SSL) connection does not interfere with the server's ability to accept other
    /// connections.
    func testIssue1143() {
        do {
            let server: HTTPServer
            let serverPort: Int
            (server, serverPort) = try startEphemeralServer(ClientE2ETests.TestServerDelegate(), useSSL: true)
            defer {
                server.stop()
            }

            var badClient = try BadClient()
            var goodClient = try GoodClient()

            /// Connect a 'bad' (non-SSL) client to the server
            try badClient.connect(serverPort, expectation: self.expectation(description: "Connecting a bad client"))
            XCTAssertEqual(badClient.connectedPort, serverPort, "BadClient not connected to expected server port")
            //XCTAssertFalse(badClient.socket.isSecure, "Expected BadClient socket to be insecure")

            /// Connect a 'good' (SSL enabled) client to the server
            try goodClient.connect(serverPort, expectation: self.expectation(description: "Connecting a bad client"))
            XCTAssertEqual(goodClient.connectedPort, serverPort, "GoodClient not connected to expected server port")
            //XCTAssertTrue(goodClient.socket.isSecure, "Expected GoodClient socket to be secure")
        } catch {
             XCTFail("Error: \(error)")
        }

        waitForExpectations(timeout: 10)
    }

    /// Tests that attempting to start a second HTTPServer on the same port fails.
    func testServersCollidingOnPort() {
        do {
            let server: HTTPServer
            let serverPort: Int
            (server, serverPort) = try startEphemeralServer(ClientE2ETests.TestServerDelegate(), useSSL: false)
            defer {
                server.stop()
            }

            do {
                let collidingServer: HTTPServer = try startServer(nil, port: serverPort, useSSL: false)
                defer {
                    collidingServer.stop()
                }
                XCTFail("Server unexpectedly succeeded in listening on a port already in use")
            } catch {
                XCTAssert(error is IOError, "Expected an IOError, received: \(error)")
            }

        } catch {
            XCTFail("Error: \(error)")
        }
    }

    /// Tests that attempting to start a second HTTPServer on the same port with
    /// SO_REUSEPORT enabled is successful.
    func testServersSharingPort() {
        do {
            let server: HTTPServer = try startServer(nil, port: 0, useSSL: false, allowPortReuse: true)
            defer {
                server.stop()
            }

            guard let serverPort = server.port else {
                XCTFail("Server port was not initialized")
                return
            }
            XCTAssertTrue(serverPort != 0, "Ephemeral server port not set")

            do {
                let sharingServer: HTTPServer = try startServer(nil, port: serverPort, useSSL: false, allowPortReuse: true)
                sharingServer.stop()
            } catch {
                XCTFail("Second server could not share listener port, received: \(error)")
            }

        } catch {
            XCTFail("Error: \(error)")
        }
    }

    ///Test that sending a bad request results in a `400/Bad Request` response
    ///from the server with a `Connection: Close` header
    func testBadRequest() {
        do {
            let server: HTTPServer = try startServer(nil, port: 0, useSSL: false, allowPortReuse: false)
            defer {
                server.stop()
            }

            guard let serverPort = server.port else {
                XCTFail("Server port was not initialized")
                return
            }
            XCTAssertTrue(serverPort != 0, "Ephemeral server port not set")

            var goodClient = GoodClient(with: HTTPClient(with: self.expectation(description: "Bad request error")))
            do {
                try goodClient.makeBadRequest(serverPort)
            } catch {
                Log.error("Failed to make bad request")
            }
            waitForExpectations(timeout: 10)

        } catch {
            XCTFail("Couldn't start server")
        }
    }

    /// Tests that sending a good request followed by garbage on a Keep-Alive
    /// connection results in a `200/OK` response, followed by a `400/Bad Request`
    /// response with `Connection: Close` header.
    /// This is to verify the fix introduced in Kitura-net PR #229, where a malformed
    /// request sent during a Keep-Alive session could cause the server to crash.
    func testBadRequestFollowingGoodRequest() {
        do {
            let server: HTTPServer = try startServer(nil, port: 0, useSSL: false, allowPortReuse: false)
            defer {
                server.stop()
            }
            guard let serverPort = server.port else {
                XCTFail("Server port was not initialized")
                return
            }
            XCTAssertTrue(serverPort != 0, "Ephemeral server port not set")
            var goodClient = GoodClient(with: HTTPClient(with: self.expectation(description: "Bad request error")))
            do {
                try goodClient.makeGoodRequestFollowedByBadRequest(serverPort)
            } catch {
                Log.error("Failed to make request")
            }
            waitForExpectations(timeout: 10)
        } catch {
            XCTFail("Couldn't start server")
        }
    }

    func testCustomEventLoopGroup() {
        do {
#if os(Linux)
            let numberOfCores = Int(linux_sched_getaffinity())
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: numberOfCores > 0 ? numberOfCores : System.coreCount)
#else
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
#endif
            let server = HTTPServer()
            do {
                try server.setEventLoopGroup(eventLoopGroup)
            } catch {
                XCTFail("Unable to initialize EventLoopGroup: \(error)")
            }
            let serverPort: Int = 8091
            defer {
                server.stop()
            }
            do {
                try server.listen(on: serverPort)
            } catch {
               XCTFail("Unable to start the server \(error)")
            }
            var goodClient = try GoodClient()
            // Connect a 'good' (SSL enabled) client to the server
            try goodClient.connect(serverPort, expectation: self.expectation(description: "Connecting a bad client"))
            XCTAssertEqual(goodClient.connectedPort, serverPort, "GoodClient not connected to expected server port")

            // Start a server using eventLoopGroup api provided by HTPPServer()
            let server2 = HTTPServer()
            do {
                try server2.setEventLoopGroup(server.eventLoopGroup)
            } catch {
                XCTFail("Unable to initialize EventLoopGroup: \(error)")
            }

            let serverPort2: Int = 8092
            defer {
                server2.stop()
            }
            do {
                try server2.listen(on: serverPort2)
            } catch {
                XCTFail("Unable to start the server \(error)")
            }
            var goodClient2 = try GoodClient()
            // Connect a 'good' (SSL enabled) client to the server
            try goodClient2.connect(serverPort2, expectation: self.expectation(description: "Connecting a bad client"))
            XCTAssertEqual(goodClient2.connectedPort, serverPort2, "GoodClient not connected to expected server port")
        } catch {
            XCTFail("Error: \(error)")
        }
        waitForExpectations(timeout: 10)
    }

    // Tests eventLoopGroup initialization in server after starting the server
    // If server `setEventLoopGroup` is called after function `listen()` server should throw
    // error HTTPServerError.eventLoopGroupAlreadyInitialized
    func testFailEventLoopGroupReinitialization() {
        do {
#if os(Linux)
            let numberOfCores = Int(linux_sched_getaffinity())
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: numberOfCores > 0 ? numberOfCores : System.coreCount)
#else
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
#endif
            let server = HTTPServer()
            do {
                try server.listen(on: 8093)
            } catch {
                XCTFail("Unable to start the server \(error)")
            }
            do {
                try server.setEventLoopGroup(eventLoopGroup)
            } catch {
                let httpError = error as? HTTPServerError
                XCTAssertEqual(httpError, HTTPServerError.eventLoopGroupAlreadyInitialized)
            }
        }
    }

    /// A simple client which connects to a port but sends no data
    struct BadClient {
        let clientBootstrap: ClientBootstrap

        var channel: Channel?

        var connectedPort: Int {
            return Int(channel?.remoteAddress?.port ?? 0)
        }

        init() throws {
            clientBootstrap = ClientBootstrap(group: MultiThreadedEventLoopGroup(numberOfThreads: 1))
                .channelInitializer { channel in
                    channel.pipeline.addHTTPClientHandlers()
                }
        }

        mutating func connect(_ port: Int, expectation: XCTestExpectation) throws {
            do {
                channel = try clientBootstrap.connect(host: "localhost", port: port).wait()
            } catch {
                Log.error("Failed to connect to port \(port)")
            }
            if channel?.remoteAddress != nil {
                expectation.fulfill()
            }
        }
    }

    /// A simple client based on NIOSSL, which connects to a port and performs
    /// an SSL handshake
    struct GoodClient {
        let clientBootstrap: ClientBootstrap

        var channel: Channel?

        var connectedPort: Int {
            return Int(channel?.remoteAddress?.port ?? 0)
        }

        init() throws {
            var nioSSLClientHandler: NIOSSLHandler? {
                let sslConfig = TLSConfiguration.forClient(certificateVerification: .none)
                do {
                    let sslContext = try NIOSSLContext(configuration: sslConfig)
                    return try NIOSSLClientHandler(context: sslContext, serverHostname: nil)
                } catch let error {
                    Log.error("Failed to create NIOSSLClientHandler. Error: \(error)")
                    return nil
                }
            }

            clientBootstrap = ClientBootstrap(group: MultiThreadedEventLoopGroup(numberOfThreads: 1))
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    if let nioSSLClientHandler = nioSSLClientHandler {
                        _ = channel.pipeline.addHandler(nioSSLClientHandler)
                    }
                    return channel.pipeline.addHTTPClientHandlers()
                }
        }

        init(with httpClient: HTTPClient) {
            clientBootstrap = ClientBootstrap(group: MultiThreadedEventLoopGroup(numberOfThreads: 1))
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    channel.pipeline.addHTTPClientHandlers().flatMap {
                        channel.pipeline.addHandler(httpClient)
                    }
                }
        }

        mutating func connect(_ port: Int, expectation: XCTestExpectation) throws {
            do {
                channel = try clientBootstrap.connect(host: "localhost", port: port).wait()
            } catch {
                Log.error("Failed to connect to port \(port)")
            }
            if channel?.remoteAddress != nil {
               expectation.fulfill()
            }
        }

        mutating func makeBadRequest(_ port: Int) throws {
            do {
                channel = try clientBootstrap.connect(host: "localhost", port: port).wait()
            } catch {
                Log.error("Failed to connect to port \(port)")
            }
            let request = HTTPRequestHead(version: HTTPVersion(major: 1, minor: 1), method: .GET, uri: "#/")
            _ = channel?.write(NIOAny(HTTPClientRequestPart.head(request)))
            _ = channel?.writeAndFlush(NIOAny(HTTPClientRequestPart.end(nil)))
        }

        mutating func makeGoodRequestFollowedByBadRequest(_ port: Int) throws {
            do {
                channel = try clientBootstrap.connect(host: "localhost", port: port).wait()
            } catch {
                Log.error("Failed to connect to port \(port)")
            }
            let request = HTTPRequestHead(version: HTTPVersion(major: 1, minor: 1), method: .GET, uri: "/")
            var httpHeaders = HTTPHeaders()
            httpHeaders.add(name: "Connection", value: "Keep-Alive")
            _ = channel?.write(NIOAny(HTTPClientRequestPart.head(request)))
            _ = channel?.writeAndFlush(NIOAny(HTTPClientRequestPart.end(nil)))
            sleep(1) //workaround for an apparent swift-nio issue
            let request0 = HTTPRequestHead(version: HTTPVersion(major: 1, minor: 1), method: .GET, uri: "#/")
            _ = channel?.write(NIOAny(HTTPClientRequestPart.head(request0)))
            _ = channel?.writeAndFlush(NIOAny(HTTPClientRequestPart.end(nil)))
        }
    }
}

class HTTPClient: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    private let expectation: XCTestExpectation

    init(with expectation: XCTestExpectation) {
        self.expectation = expectation
    }

    private var responses: [String] = []

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let request = self.unwrapInboundIn(data)
        switch request {
        case .head(let header):
            //We need to make sure that if the response is 400, the `Connection: close` header is set
            let connectionHeaderValue = header.headers["Connection"].first ?? ""
            if header.status == .badRequest && connectionHeaderValue.lowercased() == "close" {
                expectation.fulfill()
            }
        case .body:
            break
        case .end:
           break
        }
    }

    func errorCaught(ctx: ChannelHandlerContext, error: Error) {
        ctx.close(promise: nil)
    }
}
