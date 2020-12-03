const { app, tearDown } = require('../app');
const supertest = require('supertest');
const request = supertest(app);

jest.setTimeout(10000);
beforeAll(async function () {
    await request.post('/reset');
    await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE12345',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: '0000000000',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: 'zzzzzzzzzz',
    });
});

afterAll(function () {
    tearDown();
});

test('success', async function () {
    const response = await request.put('/company/queue?queue_id=QUEUE12345').send({
        status: 'ACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - queue_id number only', async function () {
    const response = await request.put('/company/queue?queue_id=0000000000').send({
        status: 'ACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - queue_id alphabet only', async function () {
    const response = await request.put('/company/queue?queue_id=zzzzzzzzzz').send({
        status: 'ACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - activated remains activated', async function () {
    const response = await request.put('/company/queue?queue_id=0000000000').send({
        status: 'ACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - deactivate', async function () {
    const response = await request.put('/company/queue?queue_id=zzzzzzzzzz').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - deactivated remains deactivated', async function () {
    const response = await request.put('/company/queue?queue_id=zzzzzzzzzz').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - not case sensitive', async function () {
    const response = await request.put('/company/queue?queue_id=ZzZzZzZzZz').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('success - deactivated remains deactivated', async function () {
    const response = await request.put('/company/queue?queue_id=zzzzzzzzzz').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(200);
});
test('Error - queue id not found', async function () {
    const response = await request.put('/company/queue?queue_id=2222222222').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('UNKNOWN_QUEUE');
});
test('Error - queue id less than 10 digits', async function () {
    const response = await request.put('/company/queue?queue_id=ZZZZZZZZZ').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});
test('Error - queue id more than 10 digits', async function () {
    const response = await request.put('/company/queue?queue_id=ZZZZZZZZZZZZZZZZ').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});
test('Error - queue id not alpha numeric', async function () {
    const response = await request.put('/company/queue?queue_id=ZZZZ-ZZZZZ').send({
        status: 'DEACTIVATE',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});
test('Error - status not DEACTIVATE or ACTIVATE', async function () {
    const response = await request.put('/company/queue?queue_id=ZZZZZZZZZZ').send({
        status: 'DEACTIVATED',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
test('Error - status not DEACTIVATE or ACTIVATE', async function () {
    const response = await request.put('/company/queue?queue_id=ZZZZZZZZZZ').send({
        status: 'ACTIVATED',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
