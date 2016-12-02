import { errors } from 'arsenal';

import api from '../api/api';
import pushMetrics from '../utilities/pushMetrics';
import routesUtils from './routesUtils';
import statsReport500 from '../utilities/statsReport500';

export default function routerWebsite(request, response, log, utapi,
    statsClient) {
    log.debug('routing request', { method: 'routerWebsite' });
    // only supports get and head
    if (request.method === 'GET') {
        // TODO: here or in api, handle 500 status check
        // and pushMetrics to utapi.
        return api.callApiMethod('getWebsite', request, response,
            log, (err, userErrorPageFailure, dataGetInfo, resMetaHeaders) => {
                let contentLength = 0;
                if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                    contentLength = resMetaHeaders['Content-Length'];
                }
                log.end().addDefaultFields({ contentLength });
                statsReport500(err, statsClient);
                pushMetrics(err, log, utapi, 'objectGet', request.bucketName,
                    contentLength);
                // user has their own error page
                if (err && dataGetInfo) {
                    return routesUtils.streamUserErrorPage(err, dataGetInfo,
                        response, log);
                }
                // send default error html response
                if (err) {
                    return routesUtils.errorHtmlResponse(err,
                        userErrorPageFailure, request.bucketName,
                        response, log);
                }
                // no error, stream data
                return routesUtils.responseStreamData(null, request.headers,
                    resMetaHeaders, dataGetInfo, response, null, log);
            });
    }
    if (request.method === 'HEAD') {
        // TODO: here or in api, handle 500 status check
        // and pushMetrics to utapi.
        return api.callApiMethod('headWebsite', request, log,
        (err, resHeaders) => {
            statsReport500(err, statsClient);
            pushMetrics(err, log, utapi, 'objectHead', request.bucketName);
            return routesUtils.responseContentHeaders(err, {}, resHeaders,
                                               response, log);
        });
    }
    // website endpoint only supports GET and HEAD
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteEndpoints.html
    return routesUtils.responseXMLBody(errors.MethodNotAllowed);
}
