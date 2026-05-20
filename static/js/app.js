function app() {
  return {
    page: 'library',
    loading: false,
    library: [],
    filters: ['all'],  // multi-select: ['all'], ['reading','on_hold'], ['updates','reading'], etc.
    viewMode: 'grid',  // 'grid' or 'list'

    // Library toolbar
    librarySearch: '',
    sortBy: localStorage.getItem('library_sort') || 'added_desc',
    genreFilter: '',
    authorFilter: '',
    tagFilter: '',
    typeFilter: '',

    // Bulk mode
    bulkMode: false, bulkSelected: [], bulkStatus: 'reading',

    // Sidebar (mobile)
    sidebarOpen: false,

    // Search
    searchQuery: '', searchResults: [], searchPagination: {}, searching: false, hasSearched: false,

    // Notifications
    notifications: [], unreadCount: 0,

    // Activity log
    activityLog: [],
    activityFilter: '',

    // Settings form
    sf: { pushover_user_key:'', pushover_app_token:'', pushover_enabled:'false', push_chapter_updates:'true', push_news:'false', push_reading_only:'false', updates_reading_only:'false', poll_interval_hours:'6', mangabaka_token:'', mangabaka_pat:'', mb_sync_enabled:'false', mu_enabled:'true', kmanga_email:'', kmanga_password:'', kmanga_recaptcha_token:'', komga_url:'', komga_api_key:'', komga_sync_read_progress:'false', idle_detection_enabled:'false', idle_threshold_days:'90', idle_auto_archive:'false', webhook_enabled:'false', webhook_url:'', default_page:'library', grid_density:'normal',
      // ── Display preferences ────────────────────────────────────────────
      show_source_badges:    'true',   // platform banner (MangaPlus, K Manga, etc.) on cards
      show_ratings_on_cards: 'true',   // ★ score overlay on cover image
      show_rating_votes:     'true',   // vote count next to rating (e.g. "(2.4K)")
      show_progress_bars:    'true',   // chapter progress bars on cards
      show_card_meta:        'true',   // type + year row on grid cards
      show_release_group:    'true',   // scanlation/release group name on cards + feed
      show_tags_on_cards:    'true',   // user tag chips on library cards
      show_card_controls:    'true',   // inline "Read:" input + catch-up button on cards
      default_view_mode:     'grid',   // persisted view mode: 'grid' or 'list'
      default_feed_grouped:  'false',  // persisted feed grouping: 'true' or 'false'
      // Rating input mode
      rating_input_mode:               'stars',
      // Rating source for display
      rating_source:                   'mangaupdates',
      // Reading dates display
      show_reading_dates:              'true',
      // Notes indicator on cards
      show_notes_indicator_on_cards:   'true',
      // Appearance
      accent_color:     '#7c6cff',
      font_scale:       '1',
      card_radius:      'md',
      sidebar_width:    '220',
      dim_finished_covers: 'true',
      show_recent_drops: 'true',
      // Scheduled metadata refresh
      metadata_refresh_enabled: 'false',
      metadata_refresh_interval_days: '7',
    },

    // Detail modal
    detailOpen: false, ds: null, ef: {}, detailReleases: [],
    mbRelinkOpen: false, mbRelinkQuery: '', mbRelinkResults: [], mbRelinkSearching: false, mbRelinkSearched: false,
    muReviewOpen: false, muSearchQ: '', muCandidates: [], muSearching: false, muSearched: false,

    // Add modal
    addOpen: false, addTarget: null, addForm: { current_chapter:'0', reading_status:'reading' }, adding: false,

    // Live feed
    feedReleases: [], feedTotal: 0, feedLoading: false, feedGrouped: false,

    // Statistics
    statsData: null, statsLoading: false,

    // Komga browser
    komgaBrowse: [], komgaTotal: 0, komgaPages: 0, komgaPage: 0,
    komgaLoading: false, komgaBrowseSearch: '',
    komgaReadFilter: 'IN_PROGRESS', // IN_PROGRESS, UNREAD, READ, or '' for all
    komgaSelected: [], komgaTrackMode: 'chapter', komgaSyncProgress: true,
    komgaImporting: false, komgaImportProgress: null,

    // Polling
    polling: false,
    metadataRefreshing: false,
    mbPushingAll: false,
    komgaSyncing: false,

    // System warnings
    systemWarnings: [],

    // Toasts
    toasts: [], _tid: 0,

    // Search debounce timer
    _searchTimer: null,
    // Accent color debounce timer
    _accentTimer: null,

    // Computed stats
    get stats() {
      const readingOnly = this.sf.updates_reading_only === 'true';
      return {
        updates: this.library.filter(s => s.has_update && !s.updates_hidden && (!readingOnly || s.reading_status === 'reading')).length,
        reading: this.library.filter(s=>s.reading_status==='reading').length,
        mu_linked: this.library.filter(s=>s.mu_series_id).length,
      };
    },

    applyThemeSettings() {
      const r = document.documentElement;
      // Accent color + hover variant
      const hex = this.sf.accent_color || '#7c6cff';
      r.style.setProperty('--accent', hex);
      // Compute a lighter hover variant via HSL
      const hsl = this._hexToHsl(hex);
      if (hsl) {
        const lighter = `hsl(${hsl.h}, ${hsl.s}%, ${Math.min(95, hsl.l + 15)}%)`;
        r.style.setProperty('--accent-h', lighter);
        const dim = `hsla(${hsl.h}, ${hsl.s}%, ${hsl.l}%, 0.12)`;
        r.style.setProperty('--accent-dim', dim);
      }
      // Font scale
      const scale = parseFloat(this.sf.font_scale) || 1;
      r.style.setProperty('font-size', scale + 'rem');
      // Card radius
      const radiusMap = { none: '0px', sm: '4px', md: '8px', lg: '16px', xl: '24px' };
      const radBase = radiusMap[this.sf.card_radius] || '8px';
      const radMult = { none: 0, sm: 0.5, md: 1, lg: 2, xl: 3 }[this.sf.card_radius] ?? 1;
      r.style.setProperty('--radius-sm', Math.round(4 * radMult) + 'px');
      r.style.setProperty('--radius-md', Math.round(8 * radMult) + 'px');
      r.style.setProperty('--radius-lg', Math.round(12 * radMult) + 'px');
      r.style.setProperty('--radius-xl', Math.round(16 * radMult) + 'px');
      // Sidebar width
      const sw = parseInt(this.sf.sidebar_width) || 220;
      r.style.setProperty('--sidebar-width', sw + 'px');
    },

    _hexToHsl(hex) {
      const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      if (!m) return null;
      let r = parseInt(m[1], 16) / 255;
      let g = parseInt(m[2], 16) / 255;
      let b = parseInt(m[3], 16) / 255;
      const max = Math.max(r, g, b), min = Math.min(r, g, b);
      let h, s, l = (max + min) / 2;
      if (max === min) { h = s = 0; }
      else {
        const d = max - min;
        s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
        switch (max) {
          case r: h = ((g - b) / d + (g < b ? 6 : 0)) / 6; break;
          case g: h = ((b - r) / d + 2) / 6; break;
          case b: h = ((r - g) / d + 4) / 6; break;
        }
      }
      return { h: Math.round(h * 360), s: Math.round(s * 100), l: Math.round(l * 100) };
    },

    async init() {
      // Load settings first (needed for idle detection, display prefs, etc.)
      try { const d = await this.api('/api/settings'); this.sf = {...this.sf,...d}; } catch(e) {}
      this.applyThemeSettings();
      // Apply persisted view/feed preferences immediately after settings load
      this.viewMode    = this.sf.default_view_mode    || 'grid';
      this.feedGrouped = this.sf.default_feed_grouped === 'true';
      await this.loadLibrary();
      await this.pollUnreadCount();
      // Load appropriate page based on default_page setting
      const defaultPage = this.sf.default_page || 'library';
      if (defaultPage !== 'library') {
        this.page = defaultPage;
        if (defaultPage === 'releases') { this.loadReleaseFeed().catch(()=>{}); }
        else if (defaultPage === 'notifications') { this.loadNotifications().catch(()=>{}); }
        else if (defaultPage === 'activity') { this.loadActivity().catch(()=>{}); }
        else if (defaultPage === 'stats') { this.loadStats().catch(()=>{}); }
        else if (defaultPage === 'komga') { this.loadKomgaBrowse().catch(()=>{}); }
        else if (defaultPage === 'settings') { this.loadSettings().catch(()=>{}); }
      } else {
        // Silently prefetch feed in background (skip if section is hidden)
        if (this.sf.show_recent_drops !== 'false') {
          this.loadReleaseFeed().catch(()=>{});
        }
      }
      setInterval(() => this.pollUnreadCount(), 30000);
    },

    // Computed: all genres from library
    get allGenres() {
      const set = new Set();
      this.library.forEach(s => (s.genres||[]).forEach(g => set.add(g)));
      return [...set].sort();
    },

    // Computed: all authors from library
    get allAuthors() {
      const set = new Set();
      this.library.forEach(s => (s.authors||[]).forEach(a => set.add(a)));
      return [...set].sort();
    },

    // Computed: all tags from library
    get allTags() {
      const set = new Set();
      this.library.forEach(s => (s.tags||[]).forEach(t => set.add(t)));
      return [...set].sort();
    },

    pageTitle() {
      return { library:'My Library', releases:'Live Feed', search:'Search', komga:'Komga Library', notifications:'Notifications', activity:'Activity Log', stats:'Statistics', settings:'Settings' }[this.page] || '';
    },

    // ── Metadata helpers ────────────────────────────────

    _esc(s) {
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    },

    /**
     * Format authors line: use role-aware display when author_roles is available.
     * Returns an HTML string like "Story: Oda Eiichiro · Art: Oda Eiichiro"
     * or falls back to "By AuthorA, AuthorB"
     */
    formatAuthors(s) {
      const roles = s.author_roles || [];
      if (roles.length > 0) {
        // Group by role
        const groups = {};
        for (const r of roles) {
          const name = (r.name || '').trim();
          if (!name) continue;           // skip malformed entries
          const role = r.role || 'Author';
          if (!groups[role]) groups[role] = [];
          groups[role].push(name);
        }
        const parts = Object.entries(groups)
          .filter(([, names]) => names.length > 0)
          .map(([role, names]) => `<span>${this._esc(role)}:</span> <strong>${names.map(n => this._esc(n)).join(', ')}</strong>`);
        if (parts.length > 0) return parts.join(' &nbsp;·&nbsp; ');
      }
      const authors = s.authors || [];
      if (authors.length > 0) {
        return `By <strong>${authors.map(a => this._esc(a)).join(', ')}</strong>`;
      }
      return '';
    },

    /**
     * Check if a given domain fragment already appears in the series' external_links.
     * Used to prevent showing duplicate fallback links when MB already provides them.
     */
    externalLinkExists(s, domainFragment) {
      return (s.external_links || []).some(lnk => (lnk.url || '').includes(domainFragment));
    },

    // ── Library ─────────────────────────────────────────
    async loadLibrary() {
      this.loading = true;
      try { this.library = await this.api('/api/series'); }
      catch(e) { this.toast('Failed to load library', 'error'); }
      finally { this.loading = false; }
    },

    toggleFilter(f) {
      if (f === 'all') {
        // "All" resets to show everything
        this.filters = ['all'];
        return;
      }
      // Remove 'all' if selecting a specific filter
      let arr = this.filters.filter(x => x !== 'all');
      const idx = arr.indexOf(f);
      if (idx === -1) {
        arr.push(f);
      } else {
        arr.splice(idx, 1);
      }
      // If nothing selected, revert to 'all'
      this.filters = arr.length > 0 ? arr : ['all'];
    },

    filteredLibrary() {
      let list = this.library;

      // Multi-select status / update / idle filter
      if (!this.filters.includes('all')) {
        list = list.filter(s => {
          for (const f of this.filters) {
            if (f === 'updates' && s.has_update && !s.updates_hidden && (this.sf.updates_reading_only !== 'true' || s.reading_status === 'reading')) return true;
            if (f === 'idle' && this.isIdle(s)) return true;
            if (s.reading_status === f) return true;
          }
          return false;
        });
      }

      // Text search
      if (this.librarySearch.trim()) {
        const q = this.librarySearch.toLowerCase().trim();
        list = list.filter(s => (s.title||'').toLowerCase().includes(q) || (s.native_title||'').toLowerCase().includes(q));
      }

      // Genre filter
      if (this.genreFilter) {
        list = list.filter(s => (s.genres||[]).includes(this.genreFilter));
      }

      // Author filter
      if (this.authorFilter) {
        list = list.filter(s => (s.authors||[]).includes(this.authorFilter));
      }

      // Tag filter
      if (this.tagFilter) {
        list = list.filter(s => (s.tags||[]).includes(this.tagFilter));
      }

      // Series type filter
      if (this.typeFilter) {
        list = list.filter(s => (s.series_type || 'manga') === this.typeFilter);
      }

      // Sorting
      const [field, dir] = this.sortBy.split('_');
      const mul = dir === 'desc' ? -1 : 1;
      list = [...list].sort((a, b) => {
        let va, vb;
        switch(field) {
          case 'title': return mul * (a.title||'').localeCompare(b.title||'');
          case 'rating': {
            const ratingOf = s => {
              if (this.sf.rating_source === 'mangabaka') {
                const v = parseFloat(s.rating);
                return isNaN(v) ? null : v;
              }
              return s.mu_rating ?? null;
            };
            va = ratingOf(a); vb = ratingOf(b);
            if (va === null && vb === null) return 0;
            if (va === null) return 1;
            if (vb === null) return -1;
            return mul * (va - vb);
          }
          case 'added':
            va = a.added_at || ''; vb = b.added_at || '';
            return mul * va.localeCompare(vb);
          case 'release':
            va = a.latest_release_date || ''; vb = b.latest_release_date || '';
            return mul * va.localeCompare(vb);
          case 'idle':
            // Sort by how long since last release (longest idle = desc)
            va = a.latest_release_date || a.added_at || ''; vb = b.latest_release_date || b.added_at || '';
            return mul * va.localeCompare(vb);
          case 'checked':
            va = a.last_checked || ''; vb = b.last_checked || '';
            return mul * va.localeCompare(vb);
          case 'unread':
            va = this._unreadNum(a); vb = this._unreadNum(b);
            return mul * (va - vb);
          case 'progress':
            va = this.chapterProgress(a); vb = this.chapterProgress(b);
            return mul * (va - vb);
          case 'last_read':
            va = a.last_read_at || ''; vb = b.last_read_at || '';
            return mul * va.localeCompare(vb);
          default: return 0;
        }
      });

      return list;
    },

    _unreadNum(s) {
      try {
        const latest = parseFloat(s.latest_chapter);
        const current = parseFloat(s.current_chapter || 0);
        if (!isNaN(latest) && !isNaN(current)) return Math.max(0, latest - current);
      } catch {}
      return 0;
    },

    librarySearchDebounce() {
      clearTimeout(this._searchTimer);
      this._searchTimer = setTimeout(() => {}, 150); // triggers Alpine reactivity
    },

    sortLibrary() {
      // Persist chosen sort so it survives page reloads
      localStorage.setItem('library_sort', this.sortBy);
    },

    isKomgaVolume(s) {
      // True for any series tracked by volume: native Komga (simulpub_source='komga')
      // OR soft-linked series — as long as komga_track_mode is 'volume'.
      return s && (s.komga_track_mode || 'chapter') === 'volume'
        && (s.simulpub_source === 'komga' || !!s.komga_series_id);
    },

    // Return the user's read progress for display (chapter or volume depending on series type)
    readProgress(s) {
      return this.isKomgaVolume(s)
        ? (s.current_volume || s.current_chapter || '0')
        : (s.current_chapter || '0');
    },

    // Return the correct total/denominator for volume display.
    // Native Komga volume series: latest_chapter holds the Komga-polled volume count.
    // Soft-linked volume series: latest_chapter is from a chapter source (wrong unit) —
    //   use MB total_volumes instead, or null if unknown.
    volumeTotal(s) {
      if (!s || !this.isKomgaVolume(s)) return null;
      if (s.simulpub_source === 'komga') return s.latest_chapter || null;
      return s.total_volumes || null;
    },

    chapterProgress(s) {
      // Volume series: use volumeTotal (Komga-polled or MB total_volumes) not latest_chapter
      const latest = this.isKomgaVolume(s)
        ? parseFloat(this.volumeTotal(s))
        : parseFloat(s.mu_latest_chapter || s.latest_chapter || s.total_chapters);
      // For Komga-volume series use current_volume (fallback to current_chapter during transition)
      const current = this.isKomgaVolume(s)
        ? parseFloat(s.current_volume || s.current_chapter)
        : parseFloat(s.current_chapter);
      if (!latest || isNaN(latest) || isNaN(current)) return 0;
      return Math.min(100, (current / latest) * 100);
    },

    unreadChapters(s) {
      try {
        const latest = parseFloat(s.latest_chapter);
        const current = this.isKomgaVolume(s)
          ? parseFloat(s.current_volume || s.current_chapter || 0)
          : parseFloat(s.current_chapter || 0);
        if (!isNaN(latest) && !isNaN(current)) return Math.max(0, Math.floor(latest - current));
      } catch {}
      return '?';
    },

    // ── Inline chapter/volume controls ──────────────────────────
    async quickSetChapter(s, value) {
      const val = String(value).trim();
      const isVolSeries = this.isKomgaVolume(s);
      const field = isVolSeries ? 'current_volume' : 'current_chapter';
      const current = isVolSeries ? (s.current_volume || '0') : (s.current_chapter || '0');
      if (val === current) return;
      if (val === '' || isNaN(parseFloat(val)) || parseFloat(val) < 0) return;
      try {
        await this.api(`/api/series/${s.id}`, 'PATCH', { [field]: val });
        const idx = this.library.findIndex(x => x.id === s.id);
        if (idx !== -1) {
          this.library[idx][field] = val;
          const latest = parseFloat(this.library[idx].latest_chapter || 0);
          const readVal = parseFloat(val);
          this.library[idx].has_update = latest > 0 && readVal < latest;
        }
        this.toast(`${s.title} → ${this.unitLabel(s)} ${val} read`, 'success');
      } catch(e) {
        this.toast('Failed to update progress', 'error');
      }
    },

    async markCaughtUp(s) {
      if (!s.latest_chapter) return;
      await this.quickSetChapter(s, s.latest_chapter);
    },

    // ── Idle detection ─────────────────────────────────
    isIdle(s) {
      if (this.sf.idle_detection_enabled !== 'true') return false;
      const days = parseInt(this.sf.idle_threshold_days) || 90;
      const ref = s.latest_release_date || s.last_checked;
      if (!ref) return false;
      const refDate = new Date(ref);
      const now = new Date();
      const diffDays = (now - refDate) / (1000 * 60 * 60 * 24);
      return diffDays > days && s.reading_status === 'reading';
    },

    // ── Poll health ──────────────────────────────────
    pollHealthClass(s) {
      if (!s.poll_failures) return 'ph-ok';
      if (s.poll_failures >= 3) return 'ph-error';
      return 'ph-warn';
    },
    pollHealthTitle(s) {
      if (!s.poll_failures) return 'Polling OK';
      return `${s.poll_failures} consecutive poll failure(s)` + (s.last_poll_error ? `: ${s.last_poll_error}` : '');
    },

    // ── Bulk mode ────────────────────────────────────
    toggleBulk(id) {
      const idx = this.bulkSelected.indexOf(id);
      if (idx === -1) this.bulkSelected.push(id);
      else this.bulkSelected.splice(idx, 1);
    },

    bulkSelectAll() {
      const visible = this.filteredLibrary().map(s => s.id);
      if (this.bulkSelected.length === visible.length) {
        this.bulkSelected = [];
      } else {
        this.bulkSelected = [...visible];
      }
    },

    async bulkDelete() {
      const count = this.bulkSelected.length;
      let msg;
      if (count <= 5) {
        const titles = this.bulkSelected
          .map(id => this.library.find(s => s.id === id)?.title || `#${id}`)
          .map(t => `• ${t}`)
          .join('\n');
        msg = `Delete ${count} series from your library? This cannot be undone.\n\n${titles}`;
      } else {
        msg = `Delete ${count} series from your library? This cannot be undone.`;
      }
      if (!confirm(msg)) return;
      try {
        let deleted = 0;
        for (const id of this.bulkSelected) {
          await this.api(`/api/series/${id}`, 'DELETE');
          deleted++;
        }
        this.library = this.library.filter(s => !this.bulkSelected.includes(s.id));
        this.bulkSelected = [];
        this.bulkMode = false;
        this.toast(`Deleted ${deleted} series`, 'success');
      } catch(e) { this.toast('Bulk delete failed', 'error'); }
    },

    async applyBulkStatus() {
      try {
        const resp = await this.api('/api/series/bulk/status', 'POST', {
          series_ids: this.bulkSelected,
          reading_status: this.bulkStatus,
        });
        this.toast(`Updated ${resp.updated} series to "${this.bulkStatus.replace('_',' ')}"`, 'success');
        this.bulkSelected = [];
        this.bulkMode = false;
        await this.loadLibrary();
      } catch(e) { this.toast('Bulk update failed', 'error'); }
    },

    // ── Feed grouping ────────────────────────────────
    groupedFeed() {
      const groups = {};
      for (const r of this.feedReleases) {
        if (!groups[r.series_id]) {
          groups[r.series_id] = { series_id: r.series_id, series_title: r.series_title, cover_url: r.cover_url, releases: [] };
        }
        groups[r.series_id].releases.push(r);
      }
      return Object.values(groups);
    },

    // ── Activity log ─────────────────────────────────
    async loadActivity() {
      try {
        const params = this.activityFilter ? `?action=${this.activityFilter}` : '';
        this.activityLog = await this.api(`/api/series/activity/log${params}`);
      } catch(e) { this.toast('Failed to load activity', 'error'); }
    },

    // ── Statistics ───────────────────────────────────
    async loadStats() {
      this.statsLoading = true;
      try {
        this.statsData = await this.api('/api/series/stats');
      } catch(e) {
        this.toast('Failed to load statistics', 'error');
        this.statsData = null;
      } finally {
        this.statsLoading = false;
      }
    },

    // ── Settings ─────────────────────────────────────
    async loadSettings() {
      try { const d = await this.api('/api/settings'); this.sf = {...this.sf,...d}; }
      catch(e) { this.toast('Failed to load settings', 'error'); }
    },

    // ── Export / Import ──────────────────────────────
    async exportLibrary() {
      try {
        const data = await this.api('/api/series/export/json');
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `manga-tracker-backup-${new Date().toISOString().slice(0,10)}.json`;
        a.click();
        URL.revokeObjectURL(url);
        this.toast('Library exported!', 'success');
      } catch(e) { this.toast('Export failed', 'error'); }
    },

    exportMangabaka() {
      window.location.href = '/api/export/mangabaka';
    },

    async importLibrary(event) {
      const file = event.target.files[0];
      if (!file) return;
      try {
        const text = await file.text();
        const data = JSON.parse(text);
        const series = data.series || [];
        if (!series.length) { this.toast('No series found in file', 'error'); return; }
        const resp = await this.api('/api/series/import/json', 'POST', { series, activity_log: data.activity_log || [] });
        this.toast(`Imported ${resp.imported}, skipped ${resp.skipped} duplicates`, 'success');
        await this.loadLibrary();
      } catch(e) { this.toast('Import failed: invalid file', 'error'); }
      event.target.value = '';
    },

    async fillMissingCovers() {
      try {
        const resp = await this.api('/api/series/fill-missing-covers', 'POST');
        if (resp.queued === 0) {
          this.toast('All series already have covers', 'success');
        } else {
          this.toast(`Fetching covers for ${resp.queued} series in background…`, 'success');
        }
      } catch(e) { this.toast('Failed to queue cover fetch', 'error'); }
    },

    // ── Webhook test ─────────────────────────────────
    async testWebhook() {
      try { const d = await this.api('/api/settings/test-webhook', 'POST'); this.toast(d.message||'Test sent!', 'success'); }
      catch(e) { this.toast(e.detail||'Webhook test failed', 'error'); }
    },

    async testKomga() {
      try { const d = await this.api('/api/settings/test-komga', 'POST'); this.toast(d.message||'Connected!', 'success'); }
      catch(e) { this.toast(e.detail||'Komga connection failed', 'error'); }
    },

    async komgaSyncNow() {
      if (this.komgaSyncing) return;
      this.komgaSyncing = true;
      try {
        await this.api('/api/settings/komga-sync-now', 'POST');
        this.toast('Komga sync started — runs in the background.', 'success');
        const poll = setInterval(async () => {
          try {
            const s = await this.api('/api/settings/komga-sync/status', 'GET');
            if (!s.running) {
              clearInterval(poll);
              this.komgaSyncing = false;
              this.toast(`Komga sync done — ${s.synced} series processed.`, 'success');
              await this.loadLibrary();
            }
          } catch { clearInterval(poll); this.komgaSyncing = false; }
        }, 2000);
        setTimeout(() => { this.komgaSyncing = false; }, 120000);
      } catch(e) {
        this.toast(e.detail || 'Komga sync failed', 'error');
        this.komgaSyncing = false;
      }
    },

    async testMbSync() {
      try {
        const d = await this.api('/api/settings/test-mb-sync', 'POST');
        this.toast(`Connected as ${d.username}`, 'success');
      } catch(e) { this.toast(e.detail || 'PAT invalid or connection failed', 'error'); }
    },

    async mbPull() {
      try {
        const d = await this.api('/api/settings/mb-pull', 'POST');
        this.toast(`MB pull: ${d.updated} updated, ${d.unchanged} unchanged, ${d.skipped} not tracked`, 'success');
        if (d.updated > 0) await this.loadLibrary();
      } catch(e) { this.toast(e.detail || 'MB pull failed', 'error'); }
    },

    async mbPushAll() {
      if (this.mbPushingAll) return;
      this.mbPushingAll = true;
      try {
        await this.api('/api/settings/mb-push-all', 'POST');
        this.toast('Push to MB started — runs in the background.', 'success');
        // Poll for completion so we can show final counts
        const poll = setInterval(async () => {
          try {
            const s = await this.api('/api/settings/mb-push-all/status', 'GET');
            if (!s.running) {
              clearInterval(poll);
              this.mbPushingAll = false;
              const parts = [`${s.pushed} pushed`, `${s.skipped} not in MB`];
              if (s.failed > 0) parts.push(`${s.failed} failed (rate limited)`);
              this.toast(`MB push done — ${parts.join(', ')}`, s.failed > 0 ? 'warning' : 'success');
            }
          } catch { clearInterval(poll); this.mbPushingAll = false; }
        }, 3000);
        // Safety fallback — clear flag after 5 min even if poll dies
        setTimeout(() => { this.mbPushingAll = false; }, 300000);
      } catch(e) {
        this.toast(e.detail || 'MB push failed', 'error');
        this.mbPushingAll = false;
      }
    },

    async clearKMangaSession() {
      try {
        const d = await this.api('/api/settings/kmanga/clear-session', 'POST');
        this.toast(d.message || 'Session cleared', 'success');
      } catch(e) { this.toast(e.detail || 'Failed to clear session', 'error'); }
    },

    // ── Komga Browser ─────────────────────────────────────
    async loadKomgaBrowse() {
      this.komgaLoading = true;
      try {
        const params = new URLSearchParams({
          page: this.komgaPage,
          size: 24,
          sort: 'metadata.titleSort,asc',
        });
        if (this.komgaReadFilter) params.set('read_status', this.komgaReadFilter);
        if (this.komgaBrowseSearch.trim()) params.set('search', this.komgaBrowseSearch.trim());
        const data = await this.api(`/api/komga/browse?${params}`);
        this.komgaBrowse = data.content || [];
        this.komgaTotal = data.total_elements || 0;
        this.komgaPages = data.total_pages || 0;
        this.komgaPage = data.page || 0;
        // Clear selection when changing pages/filters
        this.komgaSelected = [];
      } catch(e) {
        this.toast(e.detail || 'Failed to load Komga library', 'error');
        this.komgaBrowse = [];
      } finally {
        this.komgaLoading = false;
      }
    },

    async importFromKomga() {
      if (this.komgaSelected.length === 0) return;
      this.komgaImporting = true;
      this.komgaImportProgress = null;
      // Poll progress endpoint every 600ms while import runs
      const pollInterval = setInterval(async () => {
        try {
          const p = await this.api('/api/komga/import/progress');
          if (p.running) this.komgaImportProgress = p;
        } catch {}
      }, 600);
      try {
        const items = this.komgaSelected.map(id => ({
          komga_series_id: id,
          track_mode: this.komgaTrackMode,
          sync_progress: this.komgaSyncProgress,
        }));
        const resp = await this.api('/api/komga/import', 'POST', { items });
        const parts = [];
        if (resp.imported > 0) parts.push(`${resp.imported} imported`);
        if (resp.skipped > 0) parts.push(`${resp.skipped} already tracked`);
        if (resp.errors && resp.errors.length > 0) parts.push(`${resp.errors.length} failed`);
        this.toast(parts.join(', ') || 'Done!', resp.imported > 0 ? 'success' : 'error');
        // Refresh library and the Komga browse grid
        await this.loadLibrary();
        await this.loadKomgaBrowse();
      } catch(e) {
        this.toast(e.detail || 'Import failed', 'error');
      } finally {
        clearInterval(pollInterval);
        this.komgaImporting = false;
        this.komgaImportProgress = null;
      }
    },

    async searchKomga() {
      const q = (this.ef.komgaSearch || '').trim();
      if (!q) { this.ef.komgaResults = []; this.ef.komgaSearched = false; return; }
      this.ef.komgaSearching = true;
      try {
        const data = await this.api(`/api/komga/search?q=${encodeURIComponent(q)}`);
        this.ef.komgaResults = data.content || [];
        this.ef.komgaSearched = true;
      } catch(e) {
        this.toast(e.detail || 'Komga search failed', 'error');
        this.ef.komgaResults = [];
      }
      this.ef.komgaSearching = false;
    },

    async searchKomgaLink() {
      const q = (this.ef.komgaLinkSearch || '').trim();
      if (!q) { this.ef.komgaLinkResults = []; this.ef.komgaLinkSearched = false; return; }
      this.ef.komgaLinkSearching = true;
      try {
        const data = await this.api(`/api/komga/search?q=${encodeURIComponent(q)}`);
        this.ef.komgaLinkResults = data.content || [];
        this.ef.komgaLinkSearched = true;
      } catch(e) {
        this.toast(e.detail || 'Komga search failed', 'error');
        this.ef.komgaLinkResults = [];
      }
      this.ef.komgaLinkSearching = false;
    },

    formatVotes(n) {
      if (!n) return '';
      if (n >= 1000) return (n/1000).toFixed(1) + 'k';
      return n;
    },

    /** Return 'Vol.' for Komga volume-tracked series, 'Ch.' otherwise. */
    unitLabel(s) {
      return (s && s.simulpub_source === 'komga' && (s.komga_track_mode || 'chapter') === 'volume') ? 'Vol.' : 'Ch.';
    },

    /** Unit label for a release record — infers from group_name since releases lack track mode. */
    releaseUnit(r) {
      return (r.group_name && r.group_name.includes('(volume)')) ? 'Vol.' : 'Ch.';
    },

    /** Unit label for an activity log entry — looks up the series from the library. */
    activityUnit(entry) {
      const s = this.library.find(s => s.id === entry.series_id);
      return this.unitLabel(s);
    },

    // ── Live Feed ────────────────────────────────────────
    async loadReleaseFeed() {
      this.feedLoading = true;
      try {
        const data = await this.api('/api/releases/feed');
        this.feedReleases = data.releases || [];
        this.feedTotal = data.total_in_feed || 0;
      } catch(e) {
        // Silently fail — feed is not critical
      } finally {
        this.feedLoading = false;
      }
    },

    // ── Search ───────────────────────────────────────────
    async doSearch(page=1) {
      if (!this.searchQuery.trim()) return;
      this.searching = true; this.hasSearched = true;
      try {
        const data = await this.api(`/api/series/search?q=${encodeURIComponent(this.searchQuery)}&page=${page}`);
        this.searchResults = data.data || [];
        this.searchPagination = data.pagination || {};
      } catch(e) { this.toast('Search failed', 'error'); }
      finally { this.searching = false; }
    },

    getCoverUrl(r) {
      if (!r || !r.cover) return null;
      const c = r.cover;
      return (c.x250&&c.x250.x1)||(c.x150&&c.x150.x1)||(c.raw&&c.raw.url)||null;
    },

    quickAdd(series) {
      this.addTarget = series;
      this.addForm = { current_chapter:'0', reading_status:'reading' };
      this.addOpen = true;
    },

    async confirmAdd() {
      this.adding = true;
      try {
        // Check for similar series already tracked before committing
        const { similar } = await this.api(`/api/series/similar?title=${encodeURIComponent(this.addTarget.title)}`);
        if (similar && similar.length > 0) {
          const names = similar.map(s => `• ${s.title} (${(s.similarity*100).toFixed(0)}% match)`).join('\n');
          const proceed = confirm(`Similar series already in your library:\n${names}\n\nAdd anyway?`);
          if (!proceed) { this.adding = false; return; }
        }
        await this.api('/api/series','POST',{
          series_id: this.addTarget.id,
          current_chapter: this.addForm.current_chapter,
          reading_status: this.addForm.reading_status,
        });
        this.addTarget.is_tracked = true;
        this.addOpen = false;
        this.toast(`${this.addTarget.title} added! Linking MangaUpdates in background…`, 'success');
        await this.loadLibrary();
      } catch(e) {
        this.toast(e.detail||'Failed to add series','error');
      } finally { this.adding = false; }
    },

    // ── Detail modal ─────────────────────────────────────
    async openDetail(series) {
      this.ds = series;
      this.mbRelinkOpen = false; this.mbRelinkQuery = series.title || ''; this.mbRelinkResults = []; this.mbRelinkSearched = false;
      this.ef = {
        current_chapter: series.current_chapter||'0',
        current_volume: series.current_volume||'',
        reading_status: series.reading_status||'reading',
        notes: series.notes||'',
        tags: [...(series.tags||[])],
        newTag: '',
        notification_muted: !!series.notification_muted,
        updates_hidden: !!series.updates_hidden,
        simulpub_source: series.simulpub_source||'',
        simulpub_id: series.simulpub_id||'',
        mu_latest_chapter_manual: series.mu_latest_chapter||'',
        komgaSearch: '', komgaResults: [], komgaSearching: false, komgaSearched: false,
        // Soft-link search (separate state so it doesn't conflict with native Komga search)
        komgaLinkSearch: '', komgaLinkResults: [], komgaLinkSearching: false, komgaLinkSearched: false,
        komga_track_mode: series.komga_track_mode || 'chapter',
        komga_series_id: series.komga_series_id || '',
        komga_detect_releases: !!series.komga_detect_releases,
        komga_sync_progress: !!series.komga_sync_progress,
        user_rating: series.user_rating ?? null,
        date_started: series.date_started || '',
        date_completed: series.date_completed || '',
        date_started_source: series.date_started_source || 'auto',
        date_completed_source: series.date_completed_source || 'auto',
        date_started_overriding: false,
        date_completed_overriding: false,
        date_started_reset: false,
        date_completed_reset: false,
      };
      this.detailOpen = true;
      this.detailReleases = [];
      this.muReviewOpen = false;
      this.muSearchQ = '';
      this.muCandidates = [];
      this.muSearched = false;
      // Load release history in background
      this._loadDetailReleases(series.id, series.mu_series_id);
    },

    async openDetailById(series_id) {
      const s = this.library.find(x=>x.id===series_id);
      if (s) this.openDetail(s);
    },

    async _loadDetailReleases(series_id, mu_series_id) {
      try {
        const data = await this.api(`/api/series/${series_id}/releases`);
        const stored = data.stored || [];
        const live = data.live || [];
        // Merge stored + live, deduplicate by chapter, sort newest first
        const seen = new Set();
        const merged = [];
        for (const r of [...stored, ...live]) {
          const ch = r.chapter || r.record?.chapter;
          if (ch && seen.has(ch)) continue;
          if (ch) seen.add(ch);
          merged.push(r);
        }
        merged.sort((a, b) => {
          const dateA = a.release_date || a.record?.release_date || '';
          const dateB = b.release_date || b.record?.release_date || '';
          if (dateB !== dateA) return dateB.localeCompare(dateA);
          const chA = parseFloat(a.chapter || a.record?.chapter || 0);
          const chB = parseFloat(b.chapter || b.record?.chapter || 0);
          return chB - chA;
        });
        this.detailReleases = merged.slice(0, 15);
      } catch(e) {}
    },

    async saveDetail() {
      try {
        const body = {
          current_chapter: this.ef.current_chapter,
          current_volume: this.ef.current_volume || null,
          reading_status: this.ef.reading_status,
          notes: this.ef.notes,
          tags: this.ef.tags,
          notification_muted: this.ef.notification_muted,
          updates_hidden: this.ef.updates_hidden,
          simulpub_source: this.ef.simulpub_source,
          simulpub_id: this.ef.simulpub_id,
          // komga_track_mode is used for both native Komga series and soft-linked series
          komga_track_mode: (this.ef.simulpub_source === 'komga' || this.ef.komga_series_id)
            ? this.ef.komga_track_mode : undefined,
          komga_series_id: this.ef.komga_series_id,
          komga_detect_releases: this.ef.komga_series_id ? this.ef.komga_detect_releases : undefined,
          komga_sync_progress: this.ef.komga_series_id ? this.ef.komga_sync_progress : undefined,
          user_rating: this.ef.user_rating !== this.ds.user_rating ? this.ef.user_rating : undefined,
          clear_user_rating: this.ef.user_rating === null && this.ds.user_rating !== null,
          date_started: this.ef.date_started_overriding ? (this.ef.date_started || undefined) : undefined,
          date_completed: this.ef.date_completed_overriding ? (this.ef.date_completed || undefined) : undefined,
          clear_date_started: this.ef.date_started_overriding && !this.ef.date_started && !!this.ds.date_started,
          clear_date_completed: this.ef.date_completed_overriding && !this.ef.date_completed && !!this.ds.date_completed,
          reset_date_started: this.ef.date_started_reset,
          reset_date_completed: this.ef.date_completed_reset,
        };
        if (this.ef.simulpub_source === 'custom') {
          body.mu_latest_chapter = this.ef.mu_latest_chapter_manual;
        }
        const updated = await this.api(`/api/series/${this.ds.id}`,'PATCH', body);
        const idx = this.library.findIndex(s=>s.id===updated.id);
        if (idx!==-1) this.library[idx] = updated;
        this.ds = updated;
        this.detailOpen = false;
        this.toast('Saved!','success');
      } catch(e) { this.toast('Failed to save','error'); }
    },

    async removeSeries(id) {
      const title = this.ds?.title || 'this series';
      if (!confirm(`Remove "${title}" from your library? This will also delete its release history, notifications, and activity log entries.`)) return;
      try {
        await this.api(`/api/series/${id}`,'DELETE');
        this.library = this.library.filter(s=>s.id!==id);
        this.detailOpen = false;
        this.toast('Series removed','success');
      } catch(e) { this.toast('Failed to remove','error'); }
    },

    async searchMuCandidates(seriesId) {
      this.muSearching = true;
      this.muSearched = false;
      try {
        const q = this.muSearchQ.trim();
        const url = `/api/series/${seriesId}/mu-candidates` + (q ? `?q=${encodeURIComponent(q)}` : '');
        this.muCandidates = await this.api(url);
        this.muSearched = true;
      } catch(e) {
        this.toast(e.detail || 'MU search failed', 'error');
      }
      this.muSearching = false;
    },

    async confirmMuLink(seriesId, candidate) {
      try {
        const updated = await this.api(`/api/series/${seriesId}/confirm-mu-link`, 'POST', {
          mu_series_id: candidate.series_id,
          mu_url: candidate.url,
        });
        const idx = this.library.findIndex(s => s.id === seriesId);
        if (idx !== -1) this.library[idx] = updated;
        this.ds = updated;
        this.muReviewOpen = false;
        this.muCandidates = [];
        this.toast('MU link confirmed', 'success');
      } catch(e) {
        this.toast(e.detail || 'Failed to confirm link', 'error');
      }
    },

    async refreshSeries(id) {
      try {
        const updated = await this.api(`/api/series/${id}/refresh`,'POST');
        const idx = this.library.findIndex(s=>s.id===id);
        if (idx!==-1) this.library[idx] = updated;
        this.ds = updated;
        this.toast('Refreshed from API','success');
      } catch(e) { this.toast('Refresh failed','error'); }
    },

    // ── MB relink (Komga series) ─────────────────────────
    async searchMbRelink() {
      if (!this.mbRelinkQuery.trim()) return;
      this.mbRelinkSearching = true; this.mbRelinkSearched = false;
      try {
        const data = await this.api(`/api/series/search?q=${encodeURIComponent(this.mbRelinkQuery)}&page=1`);
        this.mbRelinkResults = (data.data || []).slice(0, 6).map(r => ({
          id: r.id,
          title: r.title,
          type: r.type,
          year: r.year,
          cover: this.getCoverUrl(r),
        }));
        this.mbRelinkSearched = true;
      } catch(e) { this.toast('MB search failed', 'error'); }
      finally { this.mbRelinkSearching = false; }
    },

    async confirmMbLink(seriesId, mbId) {
      try {
        const updated = await this.api(`/api/series/${seriesId}/link-mb`, 'POST', { mb_id: mbId });
        const idx = this.library.findIndex(s => s.id === seriesId);
        if (idx !== -1) this.library[idx] = updated;
        this.ds = updated;
        this.mbRelinkOpen = false; this.mbRelinkResults = [];
        this.toast('Linked to MangaBaka', 'success');
      } catch(e) { this.toast('Link failed', 'error'); }
    },

    async unlinkMb(seriesId) {
      try {
        await this.api(`/api/series/${seriesId}/link-mb`, 'DELETE');
        const idx = this.library.findIndex(s => s.id === seriesId);
        if (idx !== -1) { this.library[idx].mb_linked_id = null; this.library[idx].mangabaka_url = null; }
        if (this.ds && this.ds.id === seriesId) { this.ds = { ...this.ds, mb_linked_id: null, mangabaka_url: null }; }
        this.toast('MB link removed', 'success');
      } catch(e) { this.toast('Unlink failed', 'error'); }
    },

    // ── Notifications ─────────────────────────────────────
    async pollUnreadCount() {
      try {
        const data = await this.api('/api/notifications?limit=1&unread_only=true');
        this.unreadCount = data.unread_count || 0;
      } catch(e) {}
    },

    async loadNotifications() {
      try {
        const data = await this.api('/api/notifications?limit=100');
        this.notifications = data.notifications || [];
        this.unreadCount = data.unread_count || 0;
      } catch(e) { this.toast('Failed to load notifications','error'); }
    },

    async markRead(id) {
      try {
        await this.api(`/api/notifications/${id}/read`,'PATCH');
        const n = this.notifications.find(n=>n.id===id);
        if (n) n.is_read = true;
        this.unreadCount = Math.max(0, this.unreadCount-1);
      } catch(e) {}
    },

    async markAllRead() {
      try {
        await this.api('/api/notifications/read-all','POST');
        this.notifications.forEach(n=>n.is_read=true);
        this.unreadCount = 0;
      } catch(e) { this.toast('Failed','error'); }
    },

    async deleteNotif(id) {
      try {
        await this.api(`/api/notifications/${id}`,'DELETE');
        this.notifications = this.notifications.filter(n=>n.id!==id);
      } catch(e) {}
    },

    async clearAllNotifs() {
      if (!confirm('Clear all notifications?')) return;
      try {
        await this.api('/api/notifications','DELETE');
        this.notifications = []; this.unreadCount = 0;
      } catch(e) { this.toast('Failed','error'); }
    },

    // ── Settings ──────────────────────────────────────────
    async loadSettings() {
      try { const d = await this.api('/api/settings'); this.sf = {...this.sf,...d}; }
      catch(e) { this.toast('Failed to load settings','error'); }
      // Load system warnings (non-blocking)
      try { const s = await this.api('/api/settings/status'); this.systemWarnings = s.warnings || []; }
      catch(e) {}
    },

    onAccentChange(val) {
      clearTimeout(this._accentTimer);
      this._accentTimer = setTimeout(() => { this.sf.accent_color = val; this.applyThemeSettings(); }, 300);
    },

    async saveSettings() {
      try {
        await this.api('/api/settings','PATCH',this.sf);
        this.applyThemeSettings();
        this.toast('Settings saved!','success');
      }
      catch(e) { this.toast('Failed to save settings','error'); }
    },

    async testPushover() {
      try { const d = await this.api('/api/settings/test-pushover','POST'); this.toast(d.message||'Test sent!','success'); }
      catch(e) { this.toast(e.detail||'Pushover test failed','error'); }
    },

    async pollNow() {
      this.polling = true;
      try {
        await this.api('/api/settings/poll-now','POST');
        this.toast('Poll started — updating shortly…','success');
        setTimeout(async () => { await this.loadLibrary(); await this.loadReleaseFeed(); this.pollUnreadCount(); }, 6000);
        setTimeout(() => { this.polling = false; }, 10000);
      } catch(e) { this.toast('Poll failed','error'); this.polling=false; }
    },

    async refreshMetadataNow() {
      if (this.metadataRefreshing) return;
      this.metadataRefreshing = true;
      try {
        await this.api('/api/settings/refresh-metadata-now','POST');
        this.toast('Metadata refresh started — runs in the background.','success');
        setTimeout(() => { this.metadataRefreshing = false; }, 15000);
      } catch(e) {
        this.toast(e.detail || 'Metadata refresh failed','error');
        this.metadataRefreshing = false;
      }
    },

    // ── Utilities ─────────────────────────────────────────
    async api(path, method='GET', body=null) {
      const opts = { method, headers: {'Content-Type':'application/json'} };
      if (body) opts.body = JSON.stringify(body);
      const resp = await fetch(path, opts);
      const data = await resp.json().catch(()=>({}));
      if (!resp.ok) throw data;
      return data;
    },

    toast(message, type='success') {
      const id = ++this._tid;
      this.toasts.push({id, message, type});
      setTimeout(()=>{ this.toasts = this.toasts.filter(t=>t.id!==id); }, 3500);
    },

    relativeTime(iso) {
      if (!iso) return '';
      const diff = Date.now() - new Date(iso + (iso.includes('Z')?'':'Z')).getTime();
      const mins = Math.floor(diff/60000);
      if (mins < 1) return 'just now';
      if (mins < 60) return `${mins}m ago`;
      const hrs = Math.floor(mins/60);
      if (hrs < 24) return `${hrs}h ago`;
      return `${Math.floor(hrs/24)}d ago`;
    },

    stripHtml(html) {
      if (!html) return '';
      return html.replace(/<[^>]*>/g,'').replace(/&[^;]+;/g,' ').replace(/\s+/g,' ').trim();
    },
  };
}
