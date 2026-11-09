const MODES = ["AUTO", "LIVE", "SIM"];

export default function Topbar({ status, onModeChange }) {
  const { source, block, gasGwei, mode } = status;
  const dot = source === "LIVE" ? "live" : source === "SIM" ? "sim" : "";
  const label = source === "LIVE" ? "ETH Mainnet Live" : source === "SIM" ? "Simulation" : source === "ERROR" ? "RPC 오류" : "연결 중…";
  const next = MODES[(MODES.indexOf(mode) + 1) % MODES.length];

  return (
    <header className="topbar">
      <div className="brand">
        <img className="brand-logo" src="/shield.svg" alt="" />
        <span className="brand-name">Aegis Wallet</span>
        <span className="brand-sub mono">FDS · Ethereum</span>
      </div>
      <div className="net">
        <span className="pill mono"><i className={`dot ${dot}`} />{label}</span>
        <span className="pill mono">Block <b>{block ? "#" + block.toLocaleString() : "—"}</b></span>
        <span className="pill mono">Gas <b>{gasGwei != null ? gasGwei.toFixed(2) : "—"}</b> gwei</span>
        <button className="pill pill-btn mono" title="LIVE(메인넷 RPC) / SIM(시뮬레이션) 전환" onClick={() => onModeChange(next)}>
          MODE: <b>{mode}</b>
        </button>
      </div>
    </header>
  );
}
