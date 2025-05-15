const handler = require('../../index');
const event = require('../../kinesis.event.example.json');
const lambdaTester = require("lambda-tester");

it.skip("test", (done) => {
    console.log(process.env.AWS_REGION);

    lambdaTester( handler.handler )
    .event( event )
    .expectResult(( result ) => {
        console.debug('the result is ', result);
        done();
    }).catch(done);
})

