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
    await request.post('/customer/queue').send({
        customer_id: 1234567894,
        queue_id: 'QUEUE12345',
    });
    await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: '0000000000',
    });
    await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'zzzzzzzzzz',
    });
});

afterAll(function () {
    tearDown();
});

const url = '/customer/queue';

test('success', async function () {
    const response = await request.get(`${url}?customer_id=1234567890&queue_id=QUEUE12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(0);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success', async function () {
    const response = await request.get(`${url}?customer_id=1234567892&queue_id=QUEUE12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(2);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success - queueid not case sensitive', async function () {
    const response = await request.get(`${url}?customer_id=1234567892&queue_id=queUe12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(2);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success - customer minimum', async function () {
    const response = await request.get(`${url}?customer_id=1000000000&queue_id=QUEUE12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(-1);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success - customer maximum', async function () {
    const response = await request.get(`${url}?customer_id=9999999999&queue_id=QUEUE12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(-1);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success - customer maximum', async function () {
    const response = await request.get(`${url}?customer_id=9999999999&queue_id=QUEUE12345`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(5);
    expect(response.body.ahead).toEqual(-1);
    expect(response.body.status).toEqual('ACTIVE');
});

test('success - queue_id all number', async function () {
    const response = await request.get(`${url}?customer_id=9999999999&queue_id=0000000000`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(0);
    expect(response.body.ahead).toEqual(-1);
    expect(response.body.status).toEqual('INACTIVE');
});

test('success - queue_id all alphabet', async function () {
    const response = await request.get(`${url}?customer_id=9999999999&queue_id=zzzzzzzzzz`);
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('ahead');
    expect(response.body).toHaveProperty('status');
    expect(response.body.total).toEqual(0);
    expect(response.body.ahead).toEqual(-1);
    expect(response.body.status).toEqual('INACTIVE');
});

test('error - queue_id not found', async function () {
    const response = await request.get(`${url}?customer_id=9999999999&queue_id=2222222222`);
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('UNKNOWN_QUEUE');
});

test('error - customer_id too small', async function () {
    const response = await request.get(`${url}?customer_id=999999999&queue_id=2222222222`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - customer_id too big', async function () {
    const response = await request.get(`${url}?customer_id=99999999999&queue_id=2222222222`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - customer_id contain alphabets', async function () {
    const response = await request.get(`${url}?customer_id=123456789O&queue_id=2222222222`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - queue_id too short', async function () {
    const response = await request.get(`${url}?customer_id=1234567890&queue_id=abcd56789`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - queue_id too long', async function () {
    const response = await request.get(`${url}?customer_id=1234567890&queue_id=abcd5678901`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - queue_id invalid character', async function () {
    const response = await request.get(`${url}?customer_id=1234567890&queue_id=abcd-01234`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});
