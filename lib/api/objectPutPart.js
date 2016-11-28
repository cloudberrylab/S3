import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';

import services from '../services';
import constants from '../../constants';
import kms from '../kms/wrapper';
import { isBucketAuthorized, isMpuAuthorized } from
    './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';

function _getPaddedPartNumber(number) {
    return `000000${number}`.substr(-5);
}

function _getPrefixKey(splitter, objectKey, uploadId) {
    return `overview${splitter}${objectKey}${splitter}${uploadId}`;
}

/**
 * PUT part of object during a multipart upload. Steps include:
 * validating metadata for authorization, bucket existence
 * and multipart upload initiation existence,
 * store object data in datastore upon successful authorization,
 * store object location returned by datastore in metadata and
 * return the result in final cb
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - request object
 * @param {object | undefined } streamingV4Params - if v4 auth,
 * object containing accessKey, signatureFromRequest, region, scopeDate,
 * timestamp, and credentialScope
 * (to be used for streaming v4 auth if applicable)
 * @param {object} log - Werelogs logger
 * @param {function} cb - final callback to call with the result
 * @return {undefined}
 */
export default function objectPutPart(authInfo, request, streamingV4Params, log,
    cb) {
    log.debug('processing request', { method: 'objectPutPart' });

    const objectKey = request.objectKey;
    const size = request.parsedContentLength;
    const partNumber = Number.parseInt(request.query.partNumber, 10);
    const bucketName = request.bucketName;
    assert.strictEqual(typeof bucketName, 'string');

    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return cb(errors.TooManyParts);
    }

    if (!Number.isInteger(partNumber) || partNumber < 1) {
        return cb(errors.InvalidArgument);
    }

    // If part size is greater than 5GB, reject it
    if (Number.parseInt(size, 10) > 5368709120) {
        return cb(errors.EntityTooLarge);
    }

    // Note: Parts are supposed to be at least 5MB except for last part.
    // However, there is no way to know whether a part is the last part
    // since keep taking parts until get a completion request.  But can
    // expect parts of at least 5MB until last part.  Also, we check that
    // part sizes are large enough when mutlipart upload completed.

    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        requestType: 'putPart or complete',
        log,
        splitter: constants.splitter,
    };

    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectPut'
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    const canonicalID = authInfo.getCanonicalID();
    assert.strictEqual(typeof canonicalID, 'string');

    log.trace('owner canonicalid to send to data', {
        canonicalID: authInfo.getCanonicalID,
    });

    const requestType = metadataValParams.requestType;
    const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
    let splitter = constants.splitter;
    let cipherBundle = null;

    return async.waterfall([
        // Get bucket and check authorization.
        next => metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                return next(err);
            }

            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', { requestType });
                return next(errors.AccessDenied);
            }

            return next(null, bucket);
        }),
        // Get bucket server-side encryption, if it exists.
        (bucket, next) => {
            const encryption = bucket.getServerSideEncryption();

            if (encryption) {
                return kms.createCipherBundle(encryption, log, (err, data) => {
                    cipherBundle = data;
                    return next(err);
                });
            }

            return next();
        },
        // Get the MPU shadow bucket.
        next => metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err) {
                return next(errors.NoSuchUpload);
            }
            return next(null, mpuBucket);
        }),
        // Check authorization of the MPU shadow bucket.
        (mpuBucket, next) => {
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }

            const searchArgs = {
                prefix: _getPrefixKey(splitter, objectKey, uploadId),
                marker: undefined,
                delimiter: undefined,
                maxKeys: 1,
            };

            return metadata.listObject(mpuBucketName, searchArgs, log,
                (err, res) => {
                    if (err) {
                        return next(err);
                    }

                    if (res.Contents.length !== 1) {
                        return next(errors.NoSuchUpload);
                    }

                    if (!isMpuAuthorized(mpuBucket, authInfo, res,
                        requestType)) {
                        return next(errors.AccessDenied);
                    }

                    return next();
                });
        },
        // Store in data backend.
        next => {
            const objectKeyContext = {
                bucketName,
                owner: authInfo.getCanonicalID(),
                namespace: request.namespace,
            };

            services.dataStore(null, objectKeyContext, cipherBundle, request,
                size, streamingV4Params, log,
                (err, extraArg, dataGetInfo, hexDigest) =>
                next(err, dataGetInfo, hexDigest));
        },
        // Store data locations in metadata.
        (dataGetInfo, hexDigest, next) => {
            // Use an array to be consistent with objectPutCopyPart where there
            // could be multiple locations.
            const dataGetInfoArr = [dataGetInfo];
            if (cipherBundle) {
                const { algorithm, masterKeyId, cryptoScheme,
                    cipheredDataKey } = cipherBundle;
                dataGetInfoArr[0].sseAlgorithm = algorithm;
                dataGetInfoArr[0].sseMasterKeyId = masterKeyId;
                dataGetInfoArr[0].sseCryptoScheme = cryptoScheme;
                dataGetInfoArr[0].sseCipheredDataKey = cipheredDataKey;
            }

            const mdParams = {
                // We pad the partNumbers so that the parts will be sorted in
                // numerical order.
                partNumber: _getPaddedPartNumber(partNumber),
                contentMD5: hexDigest,
                size,
                uploadId,
                splitter,
            };

            services.metadataStorePart(mpuBucketName, dataGetInfoArr, mdParams,
                log, err => next(err, hexDigest));
        },
    ], (err, hexDigest) => {
        if (err) {
            log.error('error in object put part (upload part)', {
                error: err,
                method: 'objectPutPart',
            });
            return cb(err);
        }
        return cb(null, hexDigest);
    });
}
