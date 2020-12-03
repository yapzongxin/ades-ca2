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
        customer_id: 1000000001,
        queue_id: 'QUEUE12345',
    });
    await request.post('/customer/queue').send({
        customer_id: 1000000002,
        queue_id: 'QUEUE12345',
    });
});

afterAll(function () {
    tearDown();
});

const url = '/company/arrival_rate';
const now = new Date();
const from = encodeURIComponent(
    `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now
        .getDate()
        .toString()
        .padStart(2, '0')}T${now.getHours().toString().padStart(2, '0')}:${now
        .getMinutes()
        .toString()
        .padStart(2, '0')}:00+08:00`,
);

test('success', async function () {
    const response = await request.get(`${url}?queue_id=queue12345&from=${from}&duration=1`);
    expect(response.status).toBe(200);
    expect(Array.isArray(response.body)).toBe(true);
    expect(response.body.length).toBeGreaterThan(0);
    expect(response.body[0]).toHaveProperty('timestamp');
    expect(response.body[0]).toHaveProperty('count');
});

test('error - queue id does not exists ', async function () {
    const response = await request.get(`${url}?queue_id=queue12346&from=${from}&duration=1`);
    expect(response.status).toBe(404);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('UNKNOWN_QUEUE');
});

test('error - from not following format', async function () {
    const response = await request.get(`${url}?queue_id=queue12346&from=2020/01/01T1:1:1+08:00&duration=1`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - duration negative', async function () {
    const response = await request.get(`${url}?queue_id=queue12346&from=${from}&duration=-1`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});

test('error - duration NaN', async function () {
    const response = await request.get(`${url}?queue_id=queue12346&from=${from}&duration=abc`);
    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('code');
    expect(response.body.code).toEqual('INVALID_QUERY_STRING');
});
