import { errors } from 'arsenal';

import metadata from '../metadata/wrapper';
import bucketShield from './apiUtils/bucket/bucketShield';
import { parseRange } from './apiUtils/object/parseRange';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import services from '../services';
import validateHeaders from '../utilities/validateHeaders';

/**
 * _findRoutingRule - find applicable routing rule from bucket metadata
 * @param {object []} routingRules - array of routingRule objects
 * @param {object} routingRule.condition - condition or conditions
 * to satisfy for routing rule to apply
 * can have either or both of the following conditions:
 * @param {string} [routingRule.condition.keyPrefixEquals] - key prefix that
 * must match object key prefix for rule to apply
 * @param {number} [routingRule.condition.httpErrorCodeReturnedEquals] - error
 * code that must match for rule to apply
 * @param {object} routingRule.redirect - rules for redirect if condition(s)
 * satsified
 * will have at least one of the following 5 params:
 * @param {string} [routingRule.redirect.protocol] - http or https
 * @param {number} [routingRule.redirect.httpRedirectCode] - code to include
 * in redirect
 * @param {string} [routingRule.redirect.hostName] - hostname for redirect
 * redirect will only have one of the following params:
 * @param {string} [routingRule.redirect.replaceKeyPrefixWith] - string to
 * replace prefix with
 * @param {string} [routingRule.redirect.replaceKeyWith] - string to replace
 * whole key with
 * @param {string} key - object key
 * @param {number} [errCode] - error code to match if applicable
 * @return {object | undefined} redirectInfo -- comprised of all of the
 * keys/values from routingRule.redirect plus
 * a key of prefixFromRule and a value of routingRule.condition.keyPrefixEquals
 */
function _findRoutingRule(routingRules, key, errCode) {
    if (routingRules === undefined || routingRules.length === 0) {
        return undefined;
    }
    // For AWS compat:
    // 1) use first routing rules whose conditions are satisfied
    // 2) for matching prefix no need to check closest match.  first
    // match wins
    // 3) there can be a match for a key condition with and without
    // error code condition but first one that matches will be the rule
    // used. So, if prefix foo without error and first rule has error condition,
    // will fall through to next foo rule.  But if first foo rule has
    // no error condition, will have match on first rule even if later
    // there is more specific rule with error condition.
    for (let i = 0; i < routingRules.length; i++) {
        const prefixFromRule =
            routingRules[i].condition.keyPrefixEquals;
        const errorCodeFromRule =
            routingRules[i].condition.httpErrorCodeReturnedEquals;
        if (prefixFromRule !== undefined) {
            if (!key.startsWith(prefixFromRule)) {
                // no key match, move on
                continue;
            }
            // add the prefixFromRule to the redirect info
            // so we can replaceKeyPrefixWith if that is part of redirect
            // rule
            const redirectInfo = Object.assign({ prefixFromRule },
                routingRules[i].redirect);
            // have key match so check error code match
            if (errorCodeFromRule !== undefined) {
                if (errCode === errorCodeFromRule) {
                    return redirectInfo;
                }
                // if don't match on both conditions, this is not the rule
                // for us
                continue;
            }
            // if no error code condition at all, we have found our match
            return redirectInfo;
        }
        // we have an error code condition but no key condition
        if (errorCodeFromRule !== undefined) {
            if (errCode === errorCodeFromRule) {
                const redirectInfo = Object.assign({},
                    routingRules[i].redirect);
                return redirectInfo;
            }
            continue;
        }
        return undefined;
    }
    return undefined;
}


/**
 * GET Website - Gets object or redirects
 * @param {object} request - normalized request object
 * @param {object} response - response object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
export default
function websiteGet(request, response, log, callback) {
    log.debug('processing request', { method: 'websiteGet' });
    const bucketName = request.bucketName;
    let objectKey = request.objectKey;

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.trace('error retrieving bucket metadata', { error: err });
            return callback(err);
        }
        if (bucketShield(bucket, 'objectGet')) {
            log.trace('bucket in transient/deleted state so shielding');
            return callback(errors.NoSuchBucket);
        }
        // bucket ACL's do not matter for website get since it is always the
        // get of an object. object ACL's are what matter
        const websiteConfig = bucket.getWebsiteConfiguration();

        if (websiteConfig === undefined) {
            return callback(errors.NoSuchWebsiteConfiguration);
        }
        // any errors above would be our own created generic error html
        // if have a website config, error going forward would be user's
        // redirect or error page if they set either in the config

        // handle redirect all
        if (websiteConfig.redirectAllRequestsTo !== undefined) {
            const hostName = websiteConfig.redirectAllRequestsTo.hostName;
            let protocol = websiteConfig.redirectAllRequestsTo.protocol;
            if (protocol === undefined) {
                protocol = request.connection.encrypted ? 'https' : 'http';
            }
            log.end().info('redirecting request', {
                httpCode: 302,
                redirectLocation: hostName,
            });
            response.writeHead(302, {
                Location: `${protocol}://${hostName}`,
            });
            return response.end();
        }

        // find index document if "directory" sent in request
        if (objectKey && objectKey.endsWith('/')) {
            objectKey = objectKey + websiteConfig.indexDocument;
        }
        // find index document if no key provided
        if (objectKey === undefined) {
            objectKey = websiteConfig.indexDocument;
        }
        // check whether need to redirect based on key
        const routingRules = websiteConfig.routingRules;
        const keyRoutingRule = _findRoutingRule(routingRules, objectKey);

        if (keyRoutingRule) {

        }
        let redirectRule = {
            condition: {
                // can be one or both of these.  if both, need to satisfy both
                keyPrefixEquals: undefined,
                httpErrorCodeReturnedEquals: undefined,
            },
            redirect: {
                // need at least one of the following 5
                // if only have replaceKeyWith or replaceKeyPrefixWith
                // don't actually have to redirect
                protocol: undefined,
                httpRedirectCode: undefined,
                hostName: undefined,
                // will only have on of the following
                replaceKeyPrefixWith: undefined,
                replaceKeyWith: undefined,
            },
        };


        // check acl's on key

        // get object

        // if get error, check error routing rules.

    });


    // const mdValParams = {
    //     authInfo,
    //     bucketName,
    //     objectKey,
    //     requestType: 'objectGet',
    //     log,
    // };
    //
    // services.metadataValidateAuthorization(mdValParams, (err, bucket,
    //     objMD) => {
    //     if (err) {
    //         log.debug('error processing request', { error: err });
    //         return callback(err);
    //     }
    //     if (!objMD) {
    //         return callback(errors.NoSuchKey);
    //     }
    //     const headerValResult = validateHeaders(objMD, request.headers);
    //     if (headerValResult.error) {
    //         return callback(headerValResult.error);
    //     }
    //     const responseMetaHeaders = collectResponseHeaders(objMD);
    //     // 0 bytes file
    //     if (objMD.location === null) {
    //         return callback(null, null, responseMetaHeaders);
    //     }
    //     let range;
    //     let maxContentLength;
    //     if (request.headers.range) {
    //         maxContentLength =
    //           parseInt(responseMetaHeaders['Content-Length'], 10);
    //         responseMetaHeaders['Accept-Ranges'] = 'bytes';
    //         const parseRangeRes = parseRange(request.headers.range,
    //           maxContentLength);
    //         range = parseRangeRes.range;
    //         const error = parseRangeRes.error;
    //         if (error) {
    //             return callback(error);
    //         }
    //         if (range) {
    //             // End of range should be included so + 1
    //             responseMetaHeaders['Content-Length'] =
    //                 Math.min(maxContentLength - range[0],
    //                 range[1] - range[0] + 1);
    //             responseMetaHeaders['Content-Range'] = `bytes ${range[0]}-`
    //                 + `${Math.min(maxContentLength - 1, range[1])}` +
    //                 `/${maxContentLength}`;
    //         }
    //     }
    //     // To provide for backwards compatibility before md-model-version 2,
    //     // need to handle cases where objMD.location is just a string
    //     const dataLocator = Array.isArray(objMD.location) ?
    //         objMD.location : [{ key: objMD.location }];
    //     // If have a data model before version 2, cannot support get range
    //     // for objects with multiple parts
    //     if (range && dataLocator.length > 1 &&
    //         dataLocator[0].start === undefined) {
    //         return callback(errors.NotImplemented);
    //     }
    //     if (objMD['x-amz-server-side-encryption']) {
    //         for (let i = 0; i < dataLocator.length; i++) {
    //             dataLocator[i].masterKeyId =
    //                 objMD['x-amz-server-side-encryption-aws-kms-key-id'];
    //             dataLocator[i].algorithm =
    //                 objMD['x-amz-server-side-encryption'];
    //         }
    //     }
    //     return callback(null, dataLocator, responseMetaHeaders, range);
    // });
}
