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
    await request.post('/company/queue').send({
        company_id: 1000000000,
        queue_id: 'QUEUE10000',
    });
    await request.put('/company/queue?queue_id=QUEUE10000').send({
        status: 'ACTIVATE',
    });
    await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: 'QUEUE20000',
    });
    await request.put('/company/queue?queue_id=QUEUE20000').send({
        status: 'ACTIVATE',
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

const url = '/customer/queue';

test('success', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(201);
});

test('success - multiple people joining queue', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567891,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(201);
    const response2 = await request.post(url).send({
        customer_id: 1234567892,
        queue_id: 'QUEUE12345',
    });
    expect(response2.status).toBe(201);
    const response3 = await request.post(url).send({
        customer_id: 1234567893,
        queue_id: 'QUEUE12345',
    });
    expect(response3.status).toBe(201);
    const response4 = await request.post(url).send({
        customer_id: 1234567894,
        queue_id: 'QUEUE12345',
    });
    expect(response4.status).toBe(201);
});

test('success - customer_id minimum', async function () {
    const response = await request.post(url).send({
        customer_id: 1000000000,
        queue_id: 'QUEUE10000',
    });
    expect(response.status).toBe(201);
});

test('success - customer_id maximum', async function () {
    const response = await request.post(url).send({
        customer_id: 9999999999,
        queue_id: 'QUEUE20000',
    });
    expect(response.status).toBe(201);
});

test('success - queue_id numeric only', async function () {
    const response = await request.post(url).send({
        customer_id: 9999999999,
        queue_id: '0000000000',
    });
    expect(response.status).toBe(201);
});

test('success - queue_id alphabets only', async function () {
    const response = await request.post(url).send({
        customer_id: 9999999999,
        queue_id: 'zzzzzzzzzz',
    });
    expect(response.status).toBe(201);
});

test('Error - Queue Id dont exists', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 'ABCDEFGHIJ',
    });
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('UNKNOWN_QUEUE');
});

test('Error - Queue not activated', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: '1111111111',
    });
    expect(response.status).toBe(422);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INACTIVE_QUEUE');
});

test('Error in parameter, customer_id, less than 10 digit', async function () {
    const response = await request.post(url).send({
        customer_id: 123456789,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, customer_id, more than 10 digit', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890123456,
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, customer_id, not numeric', async function () {
    const response = await request.post(url).send({
        customer_id: '1234567890',
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, customer_id, illegal character', async function () {
    const response = await request.post(url).send({
        customer_id: '123456789O',
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, less than 10 alphanumeric', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 'QUEUE1234',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, more than 10 alphanumeric', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 'QUEUE123450',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, illegal characters', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 'QUEUE12-3450',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, not a string', async function () {
    const response = await request.post(url).send({
        customer_id: 1234567890,
        queue_id: 1234567890,
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
