const http = require('http');
const assert = require('assert');

const Browser = require('zombie');

require('babel-core/register');
const conf = require('../../../../../lib/Config').default;
const transport = conf.https ? 'https' : 'http';
const hostname = 'bucketwebsitetester.s3-website-us-east-1.amazonaws.com';

const endpoint = `${transport}://${hostname}:8000`;

// TODO: Add this endpoint in Integration for CI
// TODO: Note in Docs that for testing need to add line to local etc/hosts:
// 127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com

describe('User visits bucket website endpoint', () => {
    const browser = new Browser();

    it('should return 405 when user requests method other than get or head',
        done => {
            const options = {
                hostname,
                port: 8000,
                method: 'POST',
            };
            const req = http.request(options, res => {
                const body = [];
                res.on('data', chunk => {
                    body.push(chunk);
                });
                res.on('end', () => {
                    assert.strictEqual(res.statusCode, 405);
                    const total = body.join('');
                    assert(total.indexOf('<head><title>405 ' +
                        'Method Not Allowed</title></head>') > -1);
                    done();
                });
            });
            req.end();
        });

    it('should return 404 when no such bucket', done => {
        browser.visit(endpoint, () => {
            browser.assert.status(404);
            browser.assert.text('title', '404 Not Found');
            browser.assert.text('h1', '404 Not Found');
            browser.assert.element('#code');
            browser.assert.text('#code', 'Code: NoSuchBucket');
            browser.assert.text('#message',
                'Message: The specified bucket does not exist.');
            done();
        });
    });

    describe('with existing bucket', () => {
        before
    })
});

// Tests:
// 1) website endpoint method other than get or head X
// 2) website endpoint without a bucket name (would need separate etc/hosts
// entry -- SKIP it)
// 3) no such bucket X
// 4) no website configuration
// 5) no key in request -- uses index document
// 6) path in request without key -- uses index document
// 7) key is not public
// 8) no such key
// 9) redirect all requests with no protocol specified (should use
// same as request)
// 10) redirect all requests with protocol specified
// 11) return user's errordocument
// 12) return error page for when user's error document can't be retrieved
// 13) redirect with just error code condition
// 14) redirect with just prefix condition
// 15) redirect with error code and prefix condition
// 16) redirect with multiple condition rules and show that first one wins
// 17) redirect with protocol specified
// 18) redirect with hostname specified
// 19) reirect with replaceKeyWith specified
// 20) redirect with replaceKeyPrefixWith specified
// 21) redirect with httpRedirect Code specified
// 22) redirect with combination of redirect items applicable
