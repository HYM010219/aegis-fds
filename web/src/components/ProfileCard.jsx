import { fmt, identicon } from "../lib/util.js";

function Identicon({ address }) {
  const { fg, bg, cells } = identicon(address);
  return (
    <svg viewBox="-0.5 -0.5 6 6">
      <rect x="-0.5" y="-0.5" width="6" height="6" fill={bg} />
      <g fill={fg}>{cells.map(([x, y]) => <rect key={`${x}-${y}`} x={x} y={y} width="1" height="1" />)}</g>
    </svg>
  );
}

export default function ProfileCard({ wallet, risk, onCopy }) {
  const level = risk >= 75 ? "high" : risk >= 40 ? "mid" : "";
  return (
    <article className="card profile">
      <div className="avatar"><Identicon address={wallet.address} /></div>
      <div className="profile-body">
        <div className="profile-title">
          <span>My Wallet</span>
          <svg className="verified" viewBox="0 0 24 24">
            <path d="M12 2l2.4 2.1 3.2-.3.9 3.1 2.8 1.6-1 3 1 3-2.8 1.6-.9 3.1-3.2-.3L12 22l-2.4-2.1-3.2.3-.9-3.1L2.7 15.5l1-3-1-3 2.8-1.6.9-3.1 3.2.3L12 2Z" fill="none" stroke="#60a5fa" strokeWidth="1.5" />
            <path d="m8.5 12 2.3 2.3 4.7-4.6" stroke="#60a5fa" strokeWidth="1.6" fill="none" />
          </svg>
        </div>
        <button className="addr mono" title="클릭하여 주소 복사" onClick={onCopy}>{wallet.address}</button>
        <div className="balance">
          <span className="balance-num">{fmt(wallet.usdt)}</span>
          <span className="balance-unit">USDT</span>
          <span className="balance-eth mono">{(+wallet.eth).toFixed(4)} ETH</span>
        </div>
        <div className="badges">
          <span className={`badge badge-risk ${level}`}>ⓘ RISK: <b>{Math.round(risk)}</b></span>
          <span className="badge">KYC 인증</span>
          <span className="badge">Travel Rule 연동</span>
          <span className="badge badge-blue">VASP_KR_01</span>
        </div>
      </div>
    </article>
  );
}
