'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const navLinks = [
  { href: '/catalog', label: 'Catalog' },
  { href: '/search', label: 'Search' },
  { href: '/docs', label: 'Documentation' },
  { href: '/about', label: 'About' },
];

export default function NavBar() {
  const pathname = usePathname();

  return (
    <header
      className="sticky top-0 z-50 w-full h-14 flex items-center bg-surface-secondary border-b border-border-subtle"
    >
      <div className="w-full max-w-[1280px] mx-auto px-6 flex items-center justify-between">
        <Link
          href="/"
          className="text-base font-semibold text-text-primary tracking-tight"
        >
          Open Nova Catalog
        </Link>

        <nav className="flex items-center gap-6">
          {navLinks.map(({ href, label }) => {
            const isActive = pathname === href || pathname.startsWith(href + '/');
            return (
              <Link
                key={href}
                href={href}
                className={[
                  'text-sm font-medium transition-colors',
                  isActive
                    ? 'text-interactive'
                    : 'text-text-primary/70 hover:text-text-primary',
                ].join(' ')}
              >
                {label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
