function useAws() {
    return !!process.env['REAL_AWS'];
}

describe('test', () => {
    if (useAws()) { /* awsconf */ console.log('aws######3') } else { /* scalconf */console.log('scal######3') }

});
