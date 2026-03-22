export default async function NovaPage({
  params,
}: {
  params: Promise<{ identifier: string }>;
}) {
  const { identifier } = await params;

  return (
    <div className="py-12">
      <h1 className="text-2xl font-semibold text-text-primary">{identifier}</h1>
    </div>
  );
}
