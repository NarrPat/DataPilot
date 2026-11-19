import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ConfigProvider } from "antd";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App";
import "./styles.css";

const client = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: true } } });
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={client}>
      <ConfigProvider theme={{ token: { colorPrimary: "#186b5b", borderRadius: 8, fontFamily: "Inter, 'Microsoft YaHei', sans-serif" } }}>
        <App />
      </ConfigProvider>
    </QueryClientProvider>
  </StrictMode>,
);
