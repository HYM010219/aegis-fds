/* 공통 유틸리티 (DOM 비의존 순수 함수) */

export const fmt = (n, d = 2) =>
  Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });

export const short = (a, head = 6, tail = 4) =>
  a && a.length > head + tail + 2 ? `${a.slice(0, head)}…${a.slice(-tail)}` : a || "";

export const randHex = (len) => {
  const b = new Uint8Array(Math.ceil(len / 2));
  crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("").slice(0, len);
};
export const randAddr = () => "0x" + randHex(40);
export const randTxHash = () => "0x" + randHex(64);

// 32bit FNV-1a 해시 (멱등키·identicon 시드용)
export const hash = (str) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return (h >>> 0).toString(16).padStart(8, "0");
};

export const isAddress = (a) => /^0x[0-9a-fA-F]{40}$/.test(a || "");

// 로그정규분포 난수 (Box-Muller)
export const logNormal = (mu, sigma) => {
  const u = 1 - Math.random(), v = Math.random();
  return Math.exp(mu + sigma * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v));
};

// 주소 기반 5x5 대칭 identicon 데이터
export const identicon = (addr) => {
  const h = hash(addr) + hash(addr + "x") + hash(addr + "y");
  const hue = parseInt(h.slice(0, 3), 16) % 360;
  const cells = [];
  for (let y = 0; y < 5; y++) for (let x = 0; x < 3; x++) {
    if (parseInt(h[(y * 3 + x) % h.length], 16) % 2 === 0) {
      cells.push([x, y]);
      if (x < 2) cells.push([4 - x, y]);
    }
  }
  return { fg: `hsl(${hue} 70% 60%)`, bg: `hsl(${hue} 30% 14%)`, cells };
};

export const store = {
  get: (k, d) => { try { return JSON.parse(localStorage.getItem("aegis:" + k)) ?? d; } catch { return d; } },
  set: (k, v) => localStorage.setItem("aegis:" + k, JSON.stringify(v)),
  del: (k) => localStorage.removeItem("aegis:" + k),
};

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const nowIso = () => new Date().toISOString();
