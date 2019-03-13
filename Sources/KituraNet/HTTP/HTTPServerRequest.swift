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
import LoggerAPI
import Foundation

/// This class implements the `ServerRequest` protocol for incoming sockets that
/// are communicating via the HTTP protocol.
public class HTTPServerRequest: ServerRequest {

    /// Set of HTTP headers of the request.
    public var headers: HeadersContainer

    @available(*, deprecated, message: "This contains just the path and query parameters starting with '/'. use 'urlURL' instead")
    public var urlString: String {
        return _urlString
    }

    /// The URL from the request in UTF-8 form
    /// This contains just the path and query parameters starting with '/'
    /// Use 'urlURL' for the full URL
    public var url: Data {
        //The url needs to retain the percent encodings. URL.path doesn't, so we do this.
        return Data(_urlString.utf8)
    }

    @available(*, deprecated, message: "URLComponents has a memory leak on linux as of swift 3.0.1. use 'urlURL' instead")
    public var urlComponents: URLComponents {
        return URLComponents(url: urlURL, resolvingAgainstBaseURL: false) ?? URLComponents()
    }

    private var _url: URL?

    private var _urlComponents: URLComponents?

    public var urlURL: URL {
        if let _url = _url {
            return _url
        }

        _urlComponents = URLComponents()
        _urlComponents?.scheme = self.enableSSL ? "https" : "http"

        var localAddress = ""
        var localAddressPort = UInt16(0)

        do {
            try ctx.eventLoop.runAndWait {
                localAddress = HTTPServerRequest.host(socketAddress: self.ctx.localAddress)
                localAddressPort = self.ctx.localAddress?.port ?? 0
            }
        } catch {
            Log.error("Unable to get the local address")
        }

        if let hostname = headers["Host"]?.first {
            _urlComponents?.host = hostname
        } else {
            Log.error("Host header not received")
            let hostname = localAddress
            _urlComponents?.host = hostname == "127.0.0.1" ? "localhost" : hostname
        }

        _urlComponents?.port = Int(localAddressPort)

        let uriComponents = _urlString.split(separator: "?")
        if uriComponents.count > 0 {
            _urlComponents?.path = String(uriComponents[0])
        }

        if uriComponents.count > 1 {
            _urlComponents?.percentEncodedQuery = String(uriComponents[1])
        }

        if let urlURL = _urlComponents?.url {
            self._url = urlURL
        } else {
            Log.error("URL init failed from: \(url)")
            self._url = URL(string: "http://not_available/")!
        }

        return self._url!
    }

    private var _remoteAddress: String?

    /// Server IP address pulled from socket.
    public var remoteAddress: String {
        if _remoteAddress == nil {
            do {
                try ctx.eventLoop.runAndWait {
                    self._remoteAddress = HTTPServerRequest.host(socketAddress: self.ctx.remoteAddress)
                }
            } catch {
                Log.error("Unable to get the remote address")
            }
        }
        return _remoteAddress!
    }

    /// Minor version of HTTP of the request
    public var httpVersionMajor: UInt16?

    /// Major version of HTTP of the request
    public var httpVersionMinor: UInt16?

    /// HTTP Method of the request.
    public var method: String

    private let ctx: ChannelHandlerContext

    private var enableSSL: Bool = false

    private var rawURLString: String

    private var urlStringPercentEncodingRemoved: String?

    private var _urlString: String {
        guard let urlStringPercentEncodingRemoved = self.urlStringPercentEncodingRemoved else {
            let _urlStringPercentEncodingRemoved = rawURLString.removingPercentEncoding ?? rawURLString
            self.urlStringPercentEncodingRemoved = _urlStringPercentEncodingRemoved
            return _urlStringPercentEncodingRemoved
        }
        return urlStringPercentEncodingRemoved
    }

    private static func host(socketAddress: SocketAddress?) -> String {
        guard let socketAddress = socketAddress else {
            return ""
        }
        switch socketAddress {
        case .v4(let addr):
            return addr.host
        case .v6(let addr):
            return addr.host
        case .unixDomainSocket:
            return "n/a"
        }
    }

    init(ctx: ChannelHandlerContext, requestHead: HTTPRequestHead, enableSSL: Bool) {
        self.ctx = ctx
        self.headers = HeadersContainer(with: requestHead.headers)
        self.method = requestHead.method.string()
        self.httpVersionMajor = requestHead.version.major
        self.httpVersionMinor = requestHead.version.minor
        self.rawURLString = requestHead.uri
        self.enableSSL = enableSSL
    }

    var buffer: BufferList?

    /// Default buffer size used for creating a BufferList
    let bufferSize = 2048

    /// Read a chunk of the body of the request.
    ///
    /// - Parameter into: An NSMutableData to hold the data in the request.
    /// - Throws: if an error occurs while reading the body.
    /// - Returns: the number of bytes read.
    public func read(into data: inout Data) throws -> Int {
        guard buffer != nil else { return 0 }
        return buffer!.fill(data: &data)
    }

    /// Read the whole body of the request.
    ///
    /// - Parameter into: An NSMutableData to hold the data in the request.
    /// - Throws: if an error occurs while reading the data.
    /// - Returns: the number of bytes read.
    public func readString() throws -> String? {
        var data = Data(capacity: bufferSize)
        let length = try read(into: &data)
        if length > 0 {
            return String(data: data, encoding: .utf8)
        } else {
            return nil
        }
    }

    /// Read the whole body of the request.
    ///
    /// - Parameter into: An NSMutableData to hold the data in the request.
    /// - Throws: if an error occurs while reading the data.
    /// - Returns: the number of bytes read.
    public func readAllData(into data: inout Data) throws -> Int {
        guard buffer != nil else { return 0 }
        var length = buffer!.fill(data: &data)
        var bytesRead = length
        while length > 0 {
            length = try read(into: &data)
            bytesRead += length
        }
        return bytesRead
    }
}

extension HTTPMethod {
    func string() -> String {
        switch self {
        case .GET:
            return "GET"
        case .PUT:
            return "PUT"
        case .ACL:
            return "ACL"
        case .HEAD:
            return "HEAD"
        case .POST:
            return "POST"
        case .COPY:
            return "COPY"
        case .LOCK:
            return "LOCK"
        case .MOVE:
            return "MOVE"
        case .BIND:
            return "BIND"
        case .LINK:
            return "LINK"
        case .PATCH:
           return "PATCH"
        case .TRACE:
           return "TRACE"
        case .MKCOL:
            return "MKCOL"
        case .MERGE:
            return "MERGE"
        case .PURGE:
            return "PURGE"
        case .NOTIFY:
            return "NOTIFY"
        case .SEARCH:
            return "SEARCH"
        case .UNLOCK:
            return "UNLOCK"
        case .REBIND:
            return "REBIND"
        case .UNBIND:
            return "UNBIND"
        case .REPORT:
            return "REPORT"
        case .DELETE:
            return "DELETE"
        case .UNLINK:
            return "UNLINK"
        case .CONNECT:
            return "CONNECT"
        case .MSEARCH:
            return "MSEARCH"
        case .OPTIONS:
            return "OPTIONS"
        case .PROPFIND:
            return "PROPFIND"
        case .CHECKOUT:
            return "CHECKOUT"
        case .PROPPATCH:
            return "PROPPATCH"
        case .SUBSCRIBE:
            return "SUBSCRIBE"
        case .MKCALENDAR:
            return "MKCALENDAR"
        case .MKACTIVITY:
            return "MKACTIVITY"
        case .UNSUBSCRIBE:
            return "UNSUBSCRIBE"
        case .RAW(let value):
            return value
        }
    }
}
