export interface Account {
  id: number;
  phone: string;
  session_name: string;
  server_group: number;
  status: 'ACTIVE' | 'TYPING' | 'RESTING' | 'FLOOD_WAIT' | 'ERROR' | 'UNAUTHORIZED' | 'DISABLED';
  api_id?: number;
  api_hash?: string;
  flood_until?: number;
  last_message_at?: string;
}

export interface Message {
  id: number;
  content: string;
  category?: string;
  is_active?: number;
}

export interface Target {
  id: number;
  username: string;
  name?: string;
  is_active?: number;
}

export interface Log {
  id: number;
  timestamp: string;
  account_phone?: string;
  server_group?: number;
  action: string;
  target?: string;
  status: 'SUCCESS' | 'WARNING' | 'ERROR' | 'INFO';
  details?: string;
}

export interface AppState {
  active_server: number;
  is_running: boolean;
  rotation_interval_minutes: number;
  elapsed_seconds: number;
  remaining_seconds: number;
  total_shift_seconds: number;
  accounts: Account[];
  messages: Message[];
  targets: Target[];
  logs: Log[];
}
