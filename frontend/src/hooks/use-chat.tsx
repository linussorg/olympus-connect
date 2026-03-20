import { createContext, useContext, useState, type ReactNode } from "react";
import type { ChatChart } from "@/lib/api";
import { translate } from "@/lib/i18n";

export interface ChatMessage {
  role: "user" | "assistant";
  text: string;
  table?: { headers: string[]; rows: string[][] };
  sql?: string;
  chart?: ChatChart;
}

interface ChatContext {
  messages: ChatMessage[];
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>;
}

const Ctx = createContext<ChatContext | null>(null);

export function ChatProvider({ children }: { children: ReactNode }) {
  const [messages, setMessages] = useState<ChatMessage[]>(() => [
    { role: "assistant", text: translate("chat.welcome") },
  ]);
  return <Ctx.Provider value={{ messages, setMessages }}>{children}</Ctx.Provider>;
}

export function useChat() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useChat must be used within ChatProvider");
  return ctx;
}
