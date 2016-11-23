import { errors } from 'arsenal';

import api from './api/api';
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
            log, (err, dataGetInfo, resMetaHeaders, range) => {
                let contentLength = 0;
                if (resMetaHeaders && resMetaHeaders['Content-Length']) {
                    contentLength = resMetaHeaders['Content-Length'];
                }
                log.end().addDefaultFields({ contentLength });
                statsReport500(err, statsClient);
                pushMetrics(err, log, utapi, 'objectGet', request.bucketName,
                    contentLength);
                return routesUtils.responseStreamData(err, request.headers,
                    resMetaHeaders, dataGetInfo, response, range, log);
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
