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
    await request.put('/company/queue?queue_id=QUEUE12345').send({
        status: 'ACTIVATE',
    });
    await request.post('/customer/queue').send({
        customer_id: 1234567890,
        queue_id: 'QUEUE12345',
    });
    await request.post('/customer/queue').send({
        customer_id: 1234567891,
        queue_id: 'QUEUE12345',
    });
    await request.post('/customer/queue').send({
        customer_id: 1234567892,
        queue_id: 'QUEUE12345',
    });
    await request.post('/customer/queue').send({
        customer_id: 1234567893,
        queue_id: 'QUEUE12345',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: '0000000000',
    });
    await request.put('/company/queue?queue_id=0000000000').send({
        status: 'ACTIVATE',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: 'zzzzzzzzzz',
    });
    await request.put('/company/queue?queue_id=zzzzzzzzzz').send({
        status: 'ACTIVATE',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: '1111111111',
    });
});

afterAll(function () {
    tearDown();
});

const url = '/company/server';

test('success', async function () {
    const response = await request.put(url).send({
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('customer_id');
    expect(response.body.customer_id).toEqual(1234567890);
});
test('success - 2nd poll', async function () {
    const response = await request.put(url).send({
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('customer_id');
    expect(response.body.customer_id).toEqual(1234567891);
});
test('success - queue_id all number', async function () {
    const response = await request.put(url).send({
        queue_id: '0000000000',
    });
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('customer_id');
    expect(response.body.customer_id).toEqual(0);
});
test('success - queue_id all character', async function () {
    const response = await request.put(url).send({
        queue_id: 'zzzzzzzzzz',
    });
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('customer_id');
    expect(response.body.customer_id).toEqual(0);
});
test('success - queue_id not case sensitive', async function () {
    const response = await request.put(url).send({
        queue_id: 'QueUE12345',
    });
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('customer_id');
    expect(response.body.customer_id).toEqual(1234567892);
});
test('Error - queue_id does not exists', async function () {
    const response = await request.put(url).send({
        queue_id: '2222222222',
    });
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('UNKNOWN_QUEUE');
});
test('Error - queue_id too short', async function () {
    const response = await request.put(url).send({
        queue_id: 'QUEUE1234',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
test('Error - queue_id too long', async function () {
    const response = await request.put(url).send({
        queue_id: 'QUEUE123456',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
test('Error - queue_id not string', async function () {
    const response = await request.put(url).send({
        queue_id: 1234567890,
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
