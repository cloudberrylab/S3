import routesUtils from './routesUtils';


export default function routeOPTIONS(request, response, log) {
    log.info('routing request', { method: 'routeOPTIONS',
                                  headers: request.rawHeaders });
    const resHeaders = {
        'Access-Control-Allow-Methods': 'GET,PUT,POST,HEAD',
        'Access-Control-Expose-Headers': 'x-amz-request-id',
        'Access-Control-Allow-Headers':
            'authorization,x-amz-date,x-amz-user-agent,Content-Type',
    };
    return routesUtils.responseNoBody(null, resHeaders, response, 200,
        log);
}
