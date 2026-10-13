// The frame every page sits in: masthead (brand, namespace, user menu), grouped
// navigation with icons and counts, skip link, and the page itself. On a wide
// screen the navigation collapses to an icon rail; below 1200 px it is a drawer.
import { useEffect, useRef, useState, type ComponentType } from "react";
import { Outlet, useNavigate, useParams, useRouterState } from "@tanstack/react-router";
import {
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
import { useSession } from "../api/queries";
import { PROTOTYPE, setCsrfToken } from "../api/client";
import { Logo } from "./Logo";
import { useTheme, type ThemeChoice } from "./theme";
import { useNavCounts, type NavCount } from "./attention";

type Entry = { path: string; label: string; icon: ComponentType };

export const NAV: { group: string | null; items: Entry[] }[] = [
  { group: null, items: [{ path: "", label: "Overview", icon: TachometerAltIcon }] },
  {
    group: "Protect",
    items: [
      { path: "backups", label: "Backups", icon: ArchiveIcon },
      { path: "schedules", label: "Schedules", icon: OutlinedClockIcon },
      { path: "protection", label: "Protection", icon: ShieldAltIcon },
    ],
  },
  {
    group: "Recover",
    items: [
      { path: "restores", label: "Restores", icon: HistoryIcon },
      { path: "switchovers", label: "Switchovers", icon: ExchangeAltIcon },
      { path: "approvals", label: "Approvals", icon: UserCheckIcon },
    ],
  },
  {
    group: "Evidence",
    items: [
      { path: "catalog", label: "Recovery catalog", icon: CatalogIcon },
      { path: "trust", label: "Trust", icon: KeyIcon },
    ],
  },
  {
    group: "Settings",
    items: [
      { path: "connections", label: "Connections", icon: PlugIcon },
      { path: "destinations", label: "Destinations", icon: CloudUploadAltIcon },
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
function UserMenu() {
  const [open, setOpen] = useState(false);
  const session = useSession();
  const { choice, setChoice } = useTheme();
  const { ns } = useParams({ strict: false });
  const actor = session.data?.actor;
  const grant = session.data?.namespaces.find((g) => g.name === ns);
  const name = actor?.displayName ?? "Signed in";
  const via = session.data?.authenticationMode === "oidc" ? "single sign-on" : "local admin";
  return (
    <Dropdown
      isOpen={open}
      onOpenChange={setOpen}
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
          <span className="lw-hide-sm">{name}</span>
        </MenuToggle>
      )}
    >
      <div className="lw-user-head">
        <div>
          <strong>{name}</strong>
        </div>
        <div className="lw-subtle lw-small">
          {actor?.subject ?? ""} · {via}
        </div>
        <div className="lw-user-head__roles">
          <div className="lw-subtle lw-small">Roles in {ns}</div>
          {grant?.roles.length ? (
            <LabelGroup numLabels={4}>
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
      <DropdownGroup label="Appearance">
        <DropdownList>
          {THEMES.map((t) => (
            <DropdownItem
              key={t.choice}
              icon={t.icon}
              onClick={() => setChoice(t.choice)}
              isSelected={choice === t.choice}
              description={t.choice === "system" ? "Follow the operating system" : undefined}
            >
              {t.label}
              {choice === t.choice && <span className="pf-v6-screen-reader"> (in use)</span>}
            </DropdownItem>
          ))}
        </DropdownList>
      </DropdownGroup>
      <Divider component="li" />
      <DropdownList>
        <DropdownItem icon={<SignOutAltIcon />} isDisabled={PROTOTYPE} description={PROTOTYPE ? "Not in the prototype" : undefined}>
          Sign out
        </DropdownItem>
      </DropdownList>
    </Dropdown>
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
  const { ns = "team-a" } = useParams({ strict: false });
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const session = useSession();
  const counts = useNavCounts(ns);
  // PatternFly's own rule: below its xl breakpoint (1200 px) the navigation is a drawer.
  const [mobile, setMobile] = useState(() => window.innerWidth < 1200);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [rail, setRail] = useState(false);

  // The CSRF token comes with the session document and lives only in memory.
  useEffect(() => setCsrfToken(session.data?.csrfToken ?? undefined), [session.data]);

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
  const names = session.data?.namespaces.map((g) => g.name) ?? [ns];

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
              <NamespaceSelect ns={ns} names={names} />
            </ToolbarItem>
            <ToolbarGroup align={{ default: "alignEnd" }} gap={{ default: "gapMd" }} alignItems="center">
              {PROTOTYPE && (
                <ToolbarItem visibility={{ default: "hidden", lg: "visible" }}>
                  <Label isCompact variant="outline" icon={<FlaskIcon />} className="lw-prototype">
                    Prototype · mock data
                  </Label>
                </ToolbarItem>
              )}
              <ToolbarItem>
                <UserMenu />
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
              const items = group.items.map((entry) => (
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
      onPageResize={(_event, { mobileView }) => {
        if (mobileView !== mobile) {
          setMobile(mobileView);
          setDrawerOpen(false);
        }
      }}
    >
      <Outlet />
    </Page>
  );
}
