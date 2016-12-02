import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';

import services from '../services';
import constants from '../../constants';
import kms from '../kms/wrapper';
import { isBucketAuthorized } from './apiUtils/authorization/aclChecks';
import metadata from '../metadata/wrapper';

// We pad the partNumbers so that the parts will be sorted in numerical order.
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
    const size = request.parsedContentLength;
    // If part size is greater than 5GB, reject it.
    // Note: Parts are supposed to be at least 5MB except for last part.
    // However, there is no way to know whether a part is the last part since
    // keep taking parts until get a completion request.  But can expect parts
    // of at least 5MB until last part.  Also, we check that part sizes are
    // large enough when mutlipart upload completed.
    if (Number.parseInt(size, 10) > 5368709120) {
        return cb(errors.EntityTooLarge);
    }
    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return cb(errors.TooManyParts);
    }
    if (!Number.isInteger(partNumber) || partNumber < 1) {
        return cb(errors.InvalidArgument);
    }
    const bucketName = request.bucketName;
    assert.strictEqual(typeof bucketName, 'string');
    const canonicalID = authInfo.getCanonicalID();
    assert.strictEqual(typeof canonicalID, 'string');
    log.trace('owner canonicalid to send to data', {
        canonicalID: authInfo.getCanonicalID,
    });
    // Note that keys in the query object retain their case, so
    // `request.query.uploadId` must be called with that exact capitalization.
    const uploadId = request.query.uploadId;
    const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
    const objectKey = request.objectKey;
    // If bucket has no server-side encryption, `cipherBundle` remains `null`.
    let cipherBundle = null;
    let splitter = constants.splitter;
    return async.waterfall([
        // Get the destination bucket.
        next => metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err && err.NoSuchBucket) {
                return next(errors.NoSuchUpload);
            }
            if (err) {
                log.error('error getting the destination bucket', {
                    error: err,
                    method: 'objectPutPart::metadata.getBucket',
                });
                return next(err);
            }
            return next(null, bucket);
        }),
        // Check the bucket authorization.
        (bucket, next) => {
            // For validating the request at the destinationBucket level the
            // `requestType` is the general 'objectPut'.
            const requestType = 'objectPut';
            if (!isBucketAuthorized(bucket, requestType, canonicalID)) {
                log.debug('access denied for user on bucket', { requestType });
                return next(errors.AccessDenied);
            }
            return next(null, bucket);
        },
        // Get bucket server-side encryption, if it exists.
        (bucket, next) => {
            const encryption = bucket.getServerSideEncryption();
            if (encryption) {
                return kms.createCipherBundle(encryption, log, (err, res) => {
                    if (err) {
                        log.error('error processing the cipher bundle for ' +
                                  'the destination bucket', {
                                      error: err,
                                  });
                    }
                    cipherBundle = res;
                    return next(err);
                });
            }
            return next();
        },
        // Get the MPU shadow bucket.
        next => metadata.getBucket(mpuBucketName, log, (err, mpuBucket) => {
            if (err && err.NoSuchBucket) {
                return next(errors.NoSuchUpload);
            }
            if (err) {
                log.error('error getting the shadow mpu bucket', {
                    error: err,
                    method: 'objectPutPart::metadata.getBucket',
                });
                return next(err);
            }
            return next(null, mpuBucket);
        }),
        // Check authorization of the MPU shadow bucket.
        (mpuBucket, next) => {
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            const mpuOverviewKey = _getPrefixKey(splitter, objectKey, uploadId);
            return metadata.getObjectMD(mpuBucketName, mpuOverviewKey, log,
                (err, res) => {
                    if (err) {
                        log.error('error getting the object from mpu bucket', {
                            error: err,
                            method: 'objectPutPart::metadata.getObjectMD',
                        });
                        return next(err);
                    }
                    const initiatorID = res.initiator.ID;
                    const requesterID = authInfo.isRequesterAnIAMUser() ?
                        authInfo.getArn() : authInfo.getCanonicalID();
                    if (initiatorID !== requesterID) {
                        return next(errors.AccessDenied);
                    }
                    return next();
                });
        },
        // Store in data backend.
        next => {
            const objectKeyContext = {
                bucketName,
                owner: canonicalID,
                namespace: request.namespace,
            };
            return services.dataStore(null, objectKeyContext, cipherBundle,
                request, size, streamingV4Params, log,
                (err, objectMetadata, dataGetInfo, hexDigest) =>
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
                partNumber: _getPaddedPartNumber(partNumber),
                contentMD5: hexDigest,
                size,
                uploadId,
                splitter,
            };
            return services.metadataStorePart(mpuBucketName, dataGetInfoArr,
                mdParams, log, err => next(err, hexDigest));
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
