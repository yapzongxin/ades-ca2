const { app, tearDown } = require('../app');
const supertest = require('supertest');
const request = supertest(app);

beforeAll(async function () {
    await request.post('/reset');
});

afterAll(function () {
    tearDown();
});

test('success', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(201);
});

test('success - company_id minimum', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1000000000,
        queue_id: 'QUEUE10000',
    });
    expect(response.status).toBe(201);
});

test('success - company_id maximum', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: 'QUEUE20000',
    });
    expect(response.status).toBe(201);
});

test('success - queue_id numeric only', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: '0000000000',
    });
    expect(response.status).toBe(201);
});

test('success - queue_id alphabets only', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 9999999999,
        queue_id: 'zzzzzzzzzz',
    });
    expect(response.status).toBe(201);
});

test('Queue Id already exists', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(422);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('QUEUE_EXISTS');
});

test('Queue Id already exists - Not case sensitive', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'queue12345',
    });
    expect(response.status).toBe(422);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('QUEUE_EXISTS');
});

test('Error in parameter, company_id, less than 10 digit', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 123456789,
        queue_id: 'QUEUE12345',
    });
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, company_id, more than 10 digit', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890123456,
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, company_id, not numeric', async function () {
    const response = await request.post('/company/queue').send({
        company_id: '1234567890',
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, company_id, illegal character', async function () {
    const response = await request.post('/company/queue').send({
        company_id: '123456789O',
        queue_id: 'QUEUE12345',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, less than 10 alphanumeric', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE1234',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, more than 10 alphanumeric', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE123450',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, illegal characters', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 'QUEUE12-3450',
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});

test('Error in parameter, queue_id, not string', async function () {
    const response = await request.post('/company/queue').send({
        company_id: 1234567890,
        queue_id: 1234567890,
    });
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_JSON_BODY');
});
