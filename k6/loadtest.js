
import http from "k6/http";
import { sleep, check } from "k6";

export let options = {
  stages: [
    { duration: "10s", target: 50 },
    { duration: "20s", target: 150 },
    { duration: "20s", target: 300 },
    { duration: "10s", target: 0 },
  ],
};

const BASE_URL = __ENV.BASE_URL;

const AMOUNT = (() => {
  const val = Number(__ENV.AMOUNT || "0.01");
  return Number.isFinite(val) && val > 0 ? val : 0.01;
})();

export default function () {
  const url = `${BASE_URL}/transfer`;

  const payload = JSON.stringify({
    fromAccountId: "A",
    toAccountId: "B",
    amount: AMOUNT,
    operationId: `op-${__VU}-${__ITER}`
  });

  const params = {
    headers: { "Content-Type": "application/json" },
  };

  const res = http.post(url, payload, params);

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(0.1);
}
