import React, { useEffect, useState, useMemo } from "react";
import "./App.css";

function useSSE(url, event) {
  const [data, setData] = useState([]);
  useEffect(() => {
    const es = new EventSource(url);
    const handler = (e) =>
      setData((d) => [JSON.parse(e.data), ...d].slice(0, 200));
    es.addEventListener(event, handler);
    return () => es.close();
  }, [url, event]);
  return data;
}

function formatMoney(v) {
  if (v == null) return "-";
  return v.toFixed(2);
}

function formatProfit(v) {
  if (v == null) return "-";
  return (v >= 0 ? "+" : "") + v.toFixed(2);
}

// 1–5 stars based on profit
function getStarCount(profit) {
  if (profit == null) return 1;
  if (profit < 5) return 2;
  if (profit < 10) return 3;
  if (profit < 20) return 4;
  return 5;
}

function Stars({ count }) {
  return (
    <div className="stars">
      {Array.from({ length: 5 }).map((_, i) => (
        <span
          key={i}
          className={i < count ? "star star--active" : "star star--inactive"}
        >
          ★
        </span>
      ))}
    </div>
  );
}

export default function App() {
  const accepted = useSSE("http://localhost:8088/sse/accepted", "accepted");
  const rejected = useSSE("http://localhost:8088/sse/rejected", "rejected");
  const orders = useSSE("http://localhost:8088/sse/orders", "order");

  // Best order per SKU (same logic you had)
  const bestBySku = useMemo(() => {
    const m = new Map();
    for (const o of orders) {
      if (!m.has(o.sku) || o.profit > m.get(o.sku).profit) m.set(o.sku, o);
    }
    return Array.from(m.values());
  }, [orders]);

  // Leaderboard = top 5 by profit
  const leaderboard = useMemo(
    () =>
      [...bestBySku]
        .sort((a, b) => (b.profit ?? 0) - (a.profit ?? 0))
        .slice(0, 5),
    [bestBySku]
  );

  return (
    <div className="app">
      <div className="app__inner">
        <header className="app__header">
          <h1 className="app__title">Live Offer Arena</h1>
          <p className="app__subtitle">
            Streaming best orders & offers from Kafka in real time.
          </p>
        </header>

        <main className="app__layout">
          {/* LEFT: Live streams */}
          <section className="panel panel--wide">
            <h2 className="panel__title">Live Streams</h2>
            <div className="panel__grid">
              <div className="subpanel">
                <h3 className="subpanel__title subpanel__title--accepted">
                  Accepted Offers
                </h3>
                <div className="table-wrapper">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>SKU</th>
                        <th>Side</th>
                        <th>Price</th>
                        <th>Profit</th>
                      </tr>
                    </thead>
                    <tbody>
                      {accepted.slice(0, 15).map((r, i) => (
                        <tr key={i}>
                          <td>{r.sku}</td>
                          <td>{r.side}</td>
                          <td>£{formatMoney(r.price)}</td>
                          <td className={r.profit >= 0 ? "pos" : "neg"}>
                            {formatProfit(r.profit)}
                          </td>
                        </tr>
                      ))}
                      {accepted.length === 0 && (
                        <tr>
                          <td colSpan={4} className="table__empty">
                            Waiting for accepted offers…
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="subpanel">
                <h3 className="subpanel__title subpanel__title--rejected">
                  Rejected Offers
                </h3>
                <div className="table-wrapper">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>SKU</th>
                        <th>Side</th>
                        <th>Price</th>
                        <th>Profit</th>
                        <th>Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rejected.slice(0, 15).map((r, i) => (
                        <tr key={i}>
                          <td>{r.sku}</td>
                          <td>{r.side}</td>
                          <td>£{formatMoney(r.price)}</td>
                          <td className={r.profit >= 0 ? "pos" : "neg"}>
                            {formatProfit(r.profit)}
                          </td>
                          <td>{r.reason}</td>
                        </tr>
                      ))}
                      {rejected.length === 0 && (
                        <tr>
                          <td colSpan={5} className="table__empty">
                            Waiting for rejected offers…
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>

          {/* RIGHT: Game-style leaderboard */}
          <section className="panel panel--leaderboard">
            <h2 className="panel__title">Top Orders Leaderboard</h2>
            <p className="panel__hint">
              Best order per SKU ranked by profit (live).
            </p>

            <div className="leaderboard">
              {leaderboard.length === 0 && (
                <div className="leaderboard__empty">
                  No orders yet. Waiting for the first champion…
                </div>
              )}

              {leaderboard.map((o, index) => {
                const stars = getStarCount(o.profit);
                const rank = index + 1;
                return (
                  <div
                    key={o.orderId ?? `${o.sku}-${index}`}
                    className={`leaderboard__row leaderboard__row--rank-${rank}`}
                  >
                    <div className="leaderboard__rank">
                      <span>#{rank}</span>
                    </div>

                    <div className="leaderboard__avatar">
                      <div className="avatar-circle">
                        {o.side === "BUY" ? "B" : "S"}
                      </div>
                    </div>

                    <div className="leaderboard__info">
                      <div className="leaderboard__sku">{o.sku}</div>
                      <div className="leaderboard__meta">
                        <span className="badge">
                          {o.side === "BUY" ? "They buy from us" : "We buy"}
                        </span>
                        <span className="badge badge--price">
                          £{formatMoney(o.unitPrice ?? o.price)}
                        </span>
                      </div>
                      <Stars count={stars} />
                    </div>

                    <div className="leaderboard__score">
                      <div className="leaderboard__score-label">Profit</div>
                      <div
                        className={
                          "leaderboard__score-value " +
                          (o.profit >= 0 ? "pos" : "neg")
                        }
                      >
                        {formatProfit(o.profit)}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        </main>
      </div>
    </div>
  );
}
