import { errors } from 'arsenal';
import V4Transform from '../../../auth/streamingV4/V4Transform';
import data from '../../../data/wrapper';

/**
 * Prepares the stream if the chunks are sent in a v4 Auth request
 * @param {object} stream - stream containing the data
 * @param {object | null } streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {RequestLogger} log - the current request logger
 * @param {function} cb - callback containing the result for V4Transform
 * @return {object} - V4Transform object if v4 Auth request, or else the stream
 */
function prepareStream(stream, streamingV4Params, log, cb) {
    if (stream.headers['x-amz-content-sha256'] ===
        'STREAMING-AWS4-HMAC-SHA256-PAYLOAD') {
        const v4Transform = new V4Transform(streamingV4Params, log, cb);
        stream.pipe(v4Transform);
        return v4Transform;
    }
    return stream;
}

/**
 * Stores object and responds back with location and storage type
 * @param {object} objectContext - object's keyContext for sproxyd Key
 * computation (put API)
 * @param {object} cipherBundle - cipher bundle that encrypt the data
 * @param {object} stream - the stream containing the data
 * @param {number} size - data size in the stream
 * @param {object | null } streamingV4Params - if v4 auth, object containing
 * accessKey, signatureFromRequest, region, scopeDate, timestamp, and
 * credentialScope (to be used for streaming v4 auth if applicable)
 * @param {RequestLogger} log - the current stream logger
 * @param {function} cb - callback containing result for the next task
 * @return {function} - calls callback with arguments:
 * error, dataRetrievalInfo, and completedHash (if any)
 */
export function dataStore(objectContext, cipherBundle, stream, size,
    streamingV4Params, log, cb) {
    const dataStream = prepareStream(stream, streamingV4Params, log, cb);
    data.put(cipherBundle, dataStream, size, objectContext, log,
        (err, dataRetrievalInfo, hashedStream) => {
            if (err) {
                log.error('error in datastore', {
                    error: err,
                });
                return cb(err);
            }
            if (!dataRetrievalInfo) {
                log.fatal('data put returned neither an error nor a key', {
                    method: 'storeObject::dataStore',
                });
                return cb(errors.InternalError);
            }
            log.trace('dataStore: backend stored key', {
                dataRetrievalInfo,
            });
            hashedStream.on('hashed', () => {
                log.trace('hashed event emitted');
            });
            const completedHash = hashedStream.completedHash;
            const contentMD5 = stream.contentMD5;
            // Check that `completedHash` matches header `contentMd5`.
            if (contentMD5 && completedHash && contentMD5 !== completedHash) {
                log.debug('contentMD5 and completedHash do not match', {
                    method: 'storeObject::dataStore',
                    completedHash,
                    contentMD5,
                });
                log.trace('contentMD5 does not match, deleting data', {
                    location: dataRetrievalInfo,
                });
                data.batchDelete(dataRetrievalInfo, log);
                return cb(errors.BadDigest);
            }
            if (completedHash) {
                hashedStream.removeAllListeners('hashed');
            }
            return cb(null, dataRetrievalInfo, completedHash);
        });
}
