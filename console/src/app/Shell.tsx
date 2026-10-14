// The frame every page sits in: masthead (brand, namespace, user menu), grouped
// navigation with icons and counts, skip link, and the page itself. On a wide
// screen the navigation collapses to an icon rail; below 1200 px it is a drawer.
import { useEffect, useRef, useState, type ComponentType } from "react";
import { Outlet, useNavigate, useParams, useRouterState } from "@tanstack/react-router";
import {
  Card,
  Divider,
  Dropdown,
  DropdownGroup,
  DropdownItem,
  DropdownList,
  Label,
  LabelGroup,
  Masthead,
  MastheadBrand,
  MastheadContent,
  MastheadLogo,
  MastheadMain,
  MastheadToggle,
  MenuToggle,
  Nav,
  NavGroup,
  NavItem,
  NavList,
  Page,
  PageSection,
  PageSidebar,
  PageSidebarBody,
  PageToggleButton,
  Select,
  SelectList,
  SelectOption,
  SkipToContent,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
  Tooltip,
} from "@patternfly/react-core";
import TachometerAltIcon from "@patternfly/react-icons/dist/esm/icons/tachometer-alt-icon";
import ArchiveIcon from "@patternfly/react-icons/dist/esm/icons/archive-icon";
import OutlinedClockIcon from "@patternfly/react-icons/dist/esm/icons/outlined-clock-icon";
import ShieldAltIcon from "@patternfly/react-icons/dist/esm/icons/shield-alt-icon";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import UserCheckIcon from "@patternfly/react-icons/dist/esm/icons/user-check-icon";
import CatalogIcon from "@patternfly/react-icons/dist/esm/icons/catalog-icon";
import KeyIcon from "@patternfly/react-icons/dist/esm/icons/key-icon";
import PlugIcon from "@patternfly/react-icons/dist/esm/icons/plug-icon";
import CloudUploadAltIcon from "@patternfly/react-icons/dist/esm/icons/cloud-upload-alt-icon";
import DesktopIcon from "@patternfly/react-icons/dist/esm/icons/desktop-icon";
import SunIcon from "@patternfly/react-icons/dist/esm/icons/sun-icon";
import MoonIcon from "@patternfly/react-icons/dist/esm/icons/moon-icon";
import FlaskIcon from "@patternfly/react-icons/dist/esm/icons/flask-icon";
import SignOutAltIcon from "@patternfly/react-icons/dist/esm/icons/sign-out-alt-icon";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import { MOCK, signOut } from "../api/client";
import { grantOf, useSessionDoc, type Flag } from "../api/session";
import { Logo } from "./Logo";
import { useTheme, type ThemeChoice } from "./theme";
import { useNavCounts, type NavCount } from "./attention";
import { shortSubject } from "./subject";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/States";

/** A navigation entry, shown when the session grants `flag` in the namespace (in any
 *  namespace, for `anyNamespace`). */
type Entry = { path: string; label: string; icon: ComponentType; flag?: Flag; anyNamespace?: boolean };

export const NAV: { group: string | null; items: Entry[] }[] = [
  { group: null, items: [{ path: "", label: "Overview", icon: TachometerAltIcon }] },
  {
    group: "Protect",
    items: [
      { path: "backups", label: "Backups", icon: ArchiveIcon, flag: "backupsRead" },
      { path: "schedules", label: "Schedules", icon: OutlinedClockIcon, flag: "schedulesRead" },
      { path: "protection", label: "Protection", icon: ShieldAltIcon, flag: "protection" },
    ],
  },
  {
    group: "Recover",
    items: [
      { path: "restores", label: "Restores", icon: HistoryIcon, flag: "restoresRead" },
      { path: "switchovers", label: "Switchovers", icon: ExchangeAltIcon, flag: "restoresRead" },
      { path: "approvals", label: "Approvals", icon: UserCheckIcon, flag: "approvalsRead" },
    ],
  },
  {
    group: "Evidence",
    items: [
      { path: "catalog", label: "Recovery catalog", icon: CatalogIcon, flag: "catalogs" },
      { path: "trust", label: "Trust", icon: KeyIcon, flag: "trustPoliciesRead", anyNamespace: true },
    ],
  },
  {
    group: "Settings",
    items: [
      { path: "connections", label: "Connections", icon: PlugIcon, flag: "connectionsRead" },
      { path: "destinations", label: "Destinations", icon: CloudUploadAltIcon, flag: "destinations" },
    ],
  },
];

const THEMES: { choice: ThemeChoice; label: string; icon: React.ReactNode }[] = [
  { choice: "system", label: "System", icon: <DesktopIcon /> },
  { choice: "light", label: "Light", icon: <SunIcon /> },
  { choice: "dark", label: "Dark", icon: <MoonIcon /> },
];

function NamespaceSelect({ ns, names }: { ns: string; names: string[] }) {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();
  return (
    <Select
      isOpen={open}
      selected={ns}
      onOpenChange={setOpen}
      onSelect={(_e, value) => {
        setOpen(false);
        if (typeof value === "string" && value !== ns) {
          void navigate({ to: "/$ns", params: { ns: value } });
        }
      }}
      toggle={(ref) => (
        <MenuToggle
          ref={ref}
          onClick={() => setOpen(!open)}
          isExpanded={open}
          aria-label={`Namespace: ${ns}. Change namespace`}
          className="lw-ns-toggle"
        >
          <span className="lw-ns-toggle__label">Namespace</span> {ns}
        </MenuToggle>
      )}
      shouldFocusToggleOnSelect
    >
      <SelectList aria-label="Namespaces">
        {names.map((name) => (
          <SelectOption key={name} value={name}>
            {name}
          </SelectOption>
        ))}
      </SelectList>
    </Select>
  );
}

function initials(name: string): string {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => w[0]!.toUpperCase())
    .join("");
}

/** The user's name, roles here, the appearance choice and Sign out, in one menu. */
function UserMenu({ ns }: { ns: string }) {
  const [open, setOpen] = useState(false);
  const [leaving, setLeaving] = useState(false);
  const session = useSessionDoc();
  const { choice, setChoice } = useTheme();
  const actor = session.actor;
  const grant = grantOf(session, ns);
  // DUI-14: the subject is an opaque identifier (often a long base64 one): shortened, whole in the title.
  const subject = shortSubject(actor.subject);
  const name = actor.displayName || subject;
  // A localAdmin console has no sign-in and so no sign-out: its one actor is the
  // administrator who started it on loopback, holding every action (docs/api.md).
  const local = session.authenticationMode === "localAdmin";
  const via = local ? "local administrator" : "single sign-on";
  return (
    <Dropdown
      isOpen={open}
      onOpenChange={setOpen}
      shouldFocusToggleOnSelect
      popperProps={{ position: "right" }}
      toggle={(ref) => (
        <MenuToggle
          ref={ref}
          onClick={() => setOpen(!open)}
          isExpanded={open}
          variant="plainText"
          aria-label={`${name}: account and appearance`}
          className="lw-user-toggle"
        >
          <span className="lw-avatar" aria-hidden="true">
            {initials(name)}
          </span>
          <span className="lw-hide-sm lw-user-toggle__name">{name}</span>
        </MenuToggle>
      )}
    >
      <div className="lw-user-head">
        <div>
          <strong className="lw-break">{name}</strong>
        </div>
        <div className="lw-subtle lw-small lw-break" title={actor.subject}>
          {name === subject ? via : `${subject} · ${via}`}
        </div>
        <div className="lw-user-head__roles">
          <div className="lw-subtle lw-small">Roles in {ns}</div>
          {local ? (
            <span className="lw-small">Every action (local administrator)</span>
          ) : grant?.roles.length ? (
            <LabelGroup numLabels={4} aria-label={`Roles in ${ns}`}>
              {grant.roles.map((r) => (
                <Label key={r} isCompact variant="outline">
                  {r}
                </Label>
              ))}
            </LabelGroup>
          ) : (
            <span className="lw-small">None</span>
          )}
        </div>
      </div>
      <Divider component="li" />
      {/* A heading in a menu is not the page's: h2, not PatternFly's default h1. */}
      <DropdownGroup label="Appearance" labelHeadingLevel="h2">
        <DropdownList>
          {THEMES.map((t) => (
            <DropdownItem
              key={t.choice}
              icon={t.icon}
              onClick={() => {
                setChoice(t.choice);
                // The choice is made: the menu closes rather than stay over the page.
                setOpen(false);
              }}
              isSelected={choice === t.choice}
              description={t.choice === "system" ? "Follow the operating system" : undefined}
            >
              {t.label}
              {choice === t.choice && <span className="pf-v6-screen-reader"> (in use)</span>}
            </DropdownItem>
          ))}
        </DropdownList>
      </DropdownGroup>
      {!local && (
        <>
          <Divider component="li" />
          <DropdownList>
            <DropdownItem
              icon={<SignOutAltIcon />}
              isDisabled={MOCK || leaving}
              description={MOCK ? "Not in the prototype" : undefined}
              onClick={() => {
                setLeaving(true);
                // Whatever the answer, reload: the next session read shows the sign-in page.
                void signOut().finally(() => window.location.reload());
              }}
            >
              Sign out
            </DropdownItem>
          </DropdownList>
        </>
      )}
    </Dropdown>
  );
}

/** A namespace in the address that the session holds no role in. */
function NoAccess({ ns, names }: { ns: string; names: string[] }) {
  return (
    <>
      <PageHeader title={ns} />
      <PageSection>
        <Card>
          <Empty title={`You hold no role in ${ns}`} icon={LockIcon}>
            The session grants you {names.length === 1 ? "one namespace" : `${names.length} namespaces`}:{" "}
            {names.map((n, i) => (
              <span key={n}>
                {i > 0 && ", "}
                <a href={`#/${n}`}>{n}</a>
              </span>
            ))}
            . Ask an administrator for a role in {ns} if you need one.
          </Empty>
        </Card>
      </PageSection>
    </>
  );
}

/** A page the navigation names whose read the session does not grant in `ns`: nothing is
 *  sent for it, and the page says why rather than wait on a read that never starts. */
function NotGranted({ ns, entry }: { ns: string; entry: Entry }) {
  return (
    <>
      <PageHeader title={entry.label} />
      <PageSection>
        <Card>
          <Empty title="Not available to you here" icon={LockIcon}>
            Your roles in {ns} do not include reading {entry.label.toLowerCase()}. Ask an administrator for a role that does if you
            need it.
          </Empty>
        </Card>
      </PageSection>
    </>
  );
}

function NavEntry({ ns, entry, current, rail, count }: { ns: string; entry: Entry; current: boolean; rail: boolean; count?: NavCount }) {
  const ref = useRef<HTMLAnchorElement>(null);
  const Icon = entry.icon;
  return (
    <NavItem
      itemId={entry.path}
      to={`#/${ns}${entry.path ? `/${entry.path}` : ""}`}
      isActive={current}
      icon={<Icon />}
      anchorRef={ref}
    >
      <span className="lw-nav__text">{entry.label}</span>
      {count && (
        <span className={`lw-count lw-count--${count.tone}`}>
          <span aria-hidden="true">{count.n}</span>
          <span className="pf-v6-screen-reader">, {count.text}</span>
        </span>
      )}
      {rail && <Tooltip content={count ? `${entry.label}: ${count.text}` : entry.label} position="right" triggerRef={ref} />}
    </NavItem>
  );
}

export function Shell() {
  const { ns = "" } = useParams({ strict: false });
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const session = useSessionDoc();
  const grant = grantOf(session, ns);
  const counts = useNavCounts(ns);
  // PatternFly's own rule: below its xl breakpoint (1200 px) the navigation is a drawer.
  const [mobile, setMobile] = useState(() => window.innerWidth < 1200);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [rail, setRail] = useState(false);

  // The drawer: Escape closes it and gives focus back to the menu button; the
  // page behind it is inert while it is open, so focus stays in the menu.
  const drawer = mobile && drawerOpen;
  useEffect(() => {
    const main = document.getElementById("lw-main");
    if (!drawer) {
      main?.removeAttribute("inert");
      return undefined;
    }
    main?.setAttribute("inert", "");
    const current = document.querySelector<HTMLElement>("#lw-sidebar .pf-v6-c-nav__link.pf-m-current");
    current?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setDrawerOpen(false);
        document.getElementById("lw-nav-toggle")?.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      main?.removeAttribute("inert");
    };
  }, [drawer]);

  const section = pathname.split("/")[2] ?? "";
  const names = session.namespaces.map((g) => g.name);
  const shown = (entry: Entry) =>
    !entry.flag || (entry.anyNamespace ? session.capabilities[entry.flag] === true : grant?.capabilities[entry.flag] === true);
  // A cluster-wide page (Trust) says for itself why it reads nothing.
  const current = NAV.flatMap((g) => g.items).find((i) => i.path === section && i.path !== "");
  const denied = current && !current.anyNamespace && !shown(current) ? current : undefined;

  const masthead = (
    <Masthead display={{ default: "inline" }}>
      <MastheadMain>
        <MastheadToggle>
          <PageToggleButton
            id="lw-nav-toggle"
            isHamburgerButton
            isSidebarOpen={mobile ? drawerOpen : !rail}
            onSidebarToggle={() => (mobile ? setDrawerOpen(!drawerOpen) : setRail(!rail))}
            aria-label={mobile ? "Main navigation" : rail ? "Expand the navigation" : "Collapse the navigation"}
          />
        </MastheadToggle>
        <MastheadBrand>
          <MastheadLogo href={`#/${ns}`} aria-label="Logweir console, overview">
            <Logo />
          </MastheadLogo>
        </MastheadBrand>
      </MastheadMain>
      <MastheadContent>
        <Toolbar isFullHeight isStatic>
          <ToolbarContent>
            <ToolbarItem>
              <NamespaceSelect ns={ns} names={grant ? names : [ns, ...names]} />
            </ToolbarItem>
            <ToolbarGroup align={{ default: "alignEnd" }} gap={{ default: "gapMd" }} alignItems="center">
              {MOCK && (
                <ToolbarItem visibility={{ default: "hidden", lg: "visible" }}>
                  <Label isCompact variant="outline" icon={<FlaskIcon />} className="lw-prototype">
                    Prototype · mock data
                  </Label>
                </ToolbarItem>
              )}
              <ToolbarItem>
                <UserMenu ns={ns} />
              </ToolbarItem>
            </ToolbarGroup>
          </ToolbarContent>
        </Toolbar>
      </MastheadContent>
    </Masthead>
  );

  const sidebar = (
    <>
      <PageSidebar isSidebarOpen={mobile ? drawerOpen : true} id="lw-sidebar">
        <PageSidebarBody>
          <Nav
            aria-label="Main"
            onSelect={() => {
              if (mobile) setDrawerOpen(false);
            }}
          >
            {NAV.map((group) => {
              const visible = group.items.filter(shown);
              if (visible.length === 0) return null;
              const items = visible.map((entry) => (
                <NavEntry
                  key={entry.path}
                  ns={ns}
                  entry={entry}
                  current={section === entry.path}
                  rail={rail && !mobile}
                  count={counts[entry.path]}
                />
              ));
              return group.group ? (
                <NavGroup key={group.group} title={group.group}>
                  {items}
                </NavGroup>
              ) : (
                <NavList key="top">{items}</NavList>
              );
            })}
          </Nav>
        </PageSidebarBody>
      </PageSidebar>
      {drawer && <div className="lw-backdrop" aria-hidden="true" onClick={() => setDrawerOpen(false)} />}
    </>
  );

  return (
    <Page
      className={rail && !mobile ? "lw-rail" : undefined}
      masthead={masthead}
      sidebar={sidebar}
      skipToContent={
        <SkipToContent
          href="#lw-main"
          onClick={(event) => {
            // The address's fragment is the route, so the skip link moves focus itself.
            event.preventDefault();
            document.getElementById("lw-main")?.focus();
          }}
        >
          Skip to content
        </SkipToContent>
      }
      mainContainerId="lw-main"
      // The main region scrolls; a page with no control in view must still scroll from
      // the keyboard (axe scrollable-region-focusable), so it is a tab stop.
      mainTabIndex={0}
      onPageResize={(_event, { mobileView }) => {
        if (mobileView !== mobile) {
          setMobile(mobileView);
          setDrawerOpen(false);
        }
      }}
    >
      {!grant ? <NoAccess ns={ns} names={names} /> : denied ? <NotGranted ns={ns} entry={denied} /> : <Outlet />}
    </Page>
  );
}
