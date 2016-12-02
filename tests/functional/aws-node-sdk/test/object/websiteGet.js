const zombie = require('zombie');

zombie.localhost('bucketwebsitetester.s3-website-us-east-1.amazonaws.com',
    8000);
// this will redirect any given host to localhost

// need to handle ci endpoint `${transport}://${process.env.IP}:8000` like
// regular testing is being done but need website endpoint env variable that
// is also in config as website endpoint.


zombie.visit('/path', function () {
    console.log(zombie.location.href);
});

// Tests:
// 1) website endpoint method other than get or head
// 2) website endpoint without a bucket name
// 3) no such
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
