import Link from 'next/link';

export default function Footer() {
  return (
    <footer className="w-full bg-surface-secondary border-t border-border-subtle py-8">
      <div className="w-full max-w-[1280px] mx-auto px-6 flex flex-col sm:flex-row items-center justify-between gap-3 text-sm text-text-tertiary">
        <span className="font-medium">Open Nova Catalog</span>
        <span>Data provided under open-access terms</span>
        <Link
          href="#"
          className="text-interactive hover:text-interactive-hover transition-colors"
        >
          GitHub
        </Link>
      </div>
    </footer>
  );
}
