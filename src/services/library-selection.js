export function isPlexLibrarySelected(config, { libraryKey = '', libraryName = '' } = {}) {
  const selectedKeys = new Set(
    (Array.isArray(config?.plex?.libraries) ? config.plex.libraries : [])
      .map((value) => String(value || '').trim())
      .filter(Boolean),
  );
  if (!selectedKeys.size) return true;

  const normalizedKey = String(libraryKey || '').trim();
  if (normalizedKey && selectedKeys.has(normalizedKey)) return true;

  const normalizedName = String(libraryName || '').trim().toLowerCase();
  if (!normalizedName) return false;

  const availableLibraries = Array.isArray(config?.plex?.availableLibraries)
    ? config.plex.availableLibraries
    : [];
  const selectedTitles = new Set(
    availableLibraries
      .filter((library) => selectedKeys.has(String(library?.key || '').trim()))
      .map((library) => String(library?.title || '').trim().toLowerCase())
      .filter(Boolean),
  );
  return selectedTitles.has(normalizedName);
}
