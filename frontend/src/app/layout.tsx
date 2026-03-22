import type { Metadata } from 'next';
import { DM_Sans, DM_Mono } from 'next/font/google';
import './globals.css';
import NavBar from '@/components/NavBar';
import Footer from '@/components/Footer';

const dmSans = DM_Sans({ subsets: ['latin'], variable: '--font-sans' });
const dmMono = DM_Mono({
  subsets: ['latin'],
  variable: '--font-mono',
  weight: ['400', '500'],
});

export const metadata: Metadata = {
  title: 'Open Nova Catalog',
  description: 'A scientific catalog of classical nova observational data.',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${dmSans.variable} ${dmMono.variable}`}>
      <body className="min-h-screen flex flex-col bg-surface-primary text-text-primary font-sans">
        <NavBar />
        <main className="flex-1 w-full max-w-[1280px] mx-auto px-6">
          {children}
        </main>
        <Footer />
      </body>
    </html>
  );
}
