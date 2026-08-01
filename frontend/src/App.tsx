import { useEffect, useState } from "react";
import AccountGrid from "./components/AccountGrid";
import Header from "./components/Header";
import LoginModal from "./components/LoginModal";
import LogsTerminal from "./components/LogsTerminal";
import ManagerPanel from "./components/ManagerPanel";
import ShiftHero from "./components/ShiftHero";
import { AppState } from "./types";

export default function App() {
  const [state, setState] = useState<AppState>({
    active_server: 1,
    is_running: false,
    rotation_interval_minutes: 10,
    elapsed_seconds: 0,
    remaining_seconds: 600,
    total_shift_seconds: 600,
    accounts: [],
    messages: [],
    targets: [],
    logs: [],
  });

  const [isLoginModalOpen, setIsLoginModalOpen] = useState(false);

  useEffect(() => {
    // Initial state fetch via REST API
    fetchState();

    // Setup WebSocket live updates
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl =
      import.meta.env.DEV
        ? `ws://${window.location.hostname}:8000/ws`
        : `${protocol}//${window.location.host}/ws`;

    const ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.data) {
          setState((prev) => ({ ...prev, ...payload.data }));
        }
      } catch (e) {
        console.error("WS Parse Error", e);
      }
    };

    ws.onerror = (e) => console.error("WS Error", e);

    return () => ws.close();
  }, []);

  const fetchState = async () => {
    try {
      const res = await fetch("/api/state");
      if (res.ok) {
        const data = await res.json();
        setState(data);
      }
    } catch (e) {
      console.error("Fetch State Error", e);
    }
  };

  const handleStartRotator = async () => {
    await fetch("/api/rotator/start", { method: "POST" });
    fetchState();
  };

  const handleStopRotator = async () => {
    await fetch("/api/rotator/stop", { method: "POST" });
    fetchState();
  };

  const handleToggleShift = async () => {
    await fetch("/api/rotator/toggle_shift", { method: "POST" });
    fetchState();
  };

  const handleAddMessage = async (content: string) => {
    await fetch("/api/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content }),
    });
    fetchState();
  };

  const handleDeleteMessage = async (id: number) => {
    await fetch(`/api/messages/${id}`, { method: "DELETE" });
    fetchState();
  };

  const handleAddTarget = async (username: string) => {
    await fetch("/api/targets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username }),
    });
    fetchState();
  };

  const handleDeleteTarget = async (id: number) => {
    await fetch(`/api/targets/${id}`, { method: "DELETE" });
    fetchState();
  };

  const handleDeleteAccount = async (id: number) => {
    await fetch(`/api/accounts/${id}`, { method: "DELETE" });
    fetchState();
  };

  return (
    <div className="app-container">
      <Header
        isRunning={state.is_running}
        activeServer={state.active_server}
        onOpenLoginModal={() => setIsLoginModalOpen(true)}
      />

      <ShiftHero
        state={state}
        onStart={handleStartRotator}
        onStop={handleStopRotator}
        onToggleShift={handleToggleShift}
      />

      <AccountGrid
        accounts={state.accounts || []}
        activeServer={state.active_server}
        onDeleteAccount={handleDeleteAccount}
      />

      <ManagerPanel
        messages={state.messages || []}
        targets={state.targets || []}
        onAddMessage={handleAddMessage}
        onDeleteMessage={handleDeleteMessage}
        onAddTarget={handleAddTarget}
        onDeleteTarget={handleDeleteTarget}
      />

      <LogsTerminal logs={state.logs || []} />

      <LoginModal
        isOpen={isLoginModalOpen}
        onClose={() => setIsLoginModalOpen(false)}
        onSuccess={fetchState}
      />
    </div>
  );
}
