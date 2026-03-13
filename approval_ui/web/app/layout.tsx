import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Spark Agent — Trade Approval",
  description: "Iron Condor trade approval dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
