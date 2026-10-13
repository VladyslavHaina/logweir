// The frame every page sits in: masthead (brand, namespace, theme, user),
// grouped navigation, skip link, and the page itself.
import { useEffect, useState } from "react";
import { Outlet, useNavigate, useParams, useRouterState } from "@tanstack/react-router";
import {
  Divider,
  Dropdown,
  DropdownGroup,
  DropdownItem,
  DropdownList,
  Label,
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
  ToggleGroup,
  ToggleGroupItem,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import SunIcon from "@patternfly/react-icons/dist/esm/icons/sun-icon";
import MoonIcon from "@patternfly/react-icons/dist/esm/icons/moon-icon";
import DesktopIcon from "@patternfly/react-icons/dist/esm/icons/desktop-icon";
import UserIcon from "@patternfly/react-icons/dist/esm/icons/user-icon";
import { useSession } from "../api/queries";
import { PROTOTYPE, setCsrfToken } from "../api/client";
import { Logo } from "./Logo";
import { useTheme, type ThemeChoice } from "./theme";

export const NAV: { group: string | null; items: { path: string; label: string }[] }[] = [
  { group: null, items: [{ path: "", label: "Overview" }] },
  {
    group: "Protect",
    items: [
      { path: "backups", label: "Backups" },
      { path: "schedules", label: "Schedules" },
      { path: "protection", label: "Protection" },
    ],
  },
  {
    group: "Recover",
    items: [
      { path: "restores", label: "Restores" },
      { path: "approvals", label: "Approvals" },
    ],
  },
  {
    group: "Evidence",
    items: [
      { path: "catalog", label: "Recovery catalog" },
      { path: "trust", label: "Trust" },
    ],
  },
  {
    group: "Settings",
    items: [
      { path: "connections", label: "Connections" },
      { path: "destinations", label: "Destinations" },
    ],
  },
];

const THEMES: { choice: ThemeChoice; label: string; icon: React.ReactNode }[] = [
  { choice: "system", label: "Follow the system", icon: <DesktopIcon /> },
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

function UserMenu() {
  const [open, setOpen] = useState(false);
  const session = useSession();
  const { choice, setChoice } = useTheme();
  const { ns } = useParams({ strict: false });
  const actor = session.data?.actor;
  const grant = session.data?.namespaces.find((g) => g.name === ns);
  return (
    <Dropdown
      isOpen={open}
      onOpenChange={setOpen}
      popperProps={{ position: "right" }}
      toggle={(ref) => (
        <MenuToggle ref={ref} onClick={() => setOpen(!open)} isExpanded={open} variant="plain" icon={<UserIcon />} aria-label="Account and settings">
          <span className="lw-hide-sm">{actor?.displayName ?? "Signed in"}</span>
        </MenuToggle>
      )}
    >
      <DropdownGroup label={actor ? `${actor.displayName} (${actor.subject})` : "Account"}>
        <DropdownList>
          <DropdownItem isDisabled description={`via ${session.data?.authenticationMode === "oidc" ? "single sign-on" : "local admin"}`}>
            Roles in {ns}: {grant?.roles.join(", ") ?? "none"}
          </DropdownItem>
        </DropdownList>
      </DropdownGroup>
      <Divider component="li" />
      <DropdownGroup label="Theme">
        <DropdownList>
          {THEMES.map((t) => (
            <DropdownItem key={t.choice} icon={t.icon} onClick={() => setChoice(t.choice)} isSelected={choice === t.choice}>
              {t.label}
            </DropdownItem>
          ))}
        </DropdownList>
      </DropdownGroup>
      <Divider component="li" />
      <DropdownList>
        <DropdownItem isDisabled={PROTOTYPE} description={PROTOTYPE ? "Not in the prototype" : undefined}>
          Sign out
        </DropdownItem>
      </DropdownList>
    </Dropdown>
  );
}

function ThemeSwitch() {
  const { choice, setChoice } = useTheme();
  return (
    <ToggleGroup isCompact aria-label="Theme">
      {THEMES.map((t) => (
        <ToggleGroupItem
          key={t.choice}
          icon={t.icon}
          aria-label={t.label}
          buttonId={`theme-${t.choice}`}
          isSelected={choice === t.choice}
          onChange={() => setChoice(t.choice)}
        />
      ))}
    </ToggleGroup>
  );
}

export function Shell() {
  const { ns = "team-a" } = useParams({ strict: false });
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const session = useSession();
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [mobile, setMobile] = useState(false);

  // The CSRF token comes with the session document and lives only in memory.
  useEffect(() => setCsrfToken(session.data?.csrfToken ?? undefined), [session.data]);

  const section = pathname.split("/")[2] ?? "";
  const names = session.data?.namespaces.map((g) => g.name) ?? [ns];

  const masthead = (
    <Masthead display={{ default: "inline" }}>
      <MastheadMain>
        <MastheadToggle>
          <PageToggleButton
            isHamburgerButton
            isSidebarOpen={sidebarOpen}
            onSidebarToggle={() => setSidebarOpen(!sidebarOpen)}
            aria-label="Main navigation"
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
            <ToolbarGroup align={{ default: "alignEnd" }} gap={{ default: "gapMd" }}>
              {PROTOTYPE && (
                <ToolbarItem visibility={{ default: "hidden", lg: "visible" }}>
                  <Label color="purple" isCompact>
                    Prototype: mock data
                  </Label>
                </ToolbarItem>
              )}
              <ToolbarItem visibility={{ default: "hidden", md: "visible" }}>
                <ThemeSwitch />
              </ToolbarItem>
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
    <PageSidebar isSidebarOpen={sidebarOpen} id="lw-sidebar">
      <PageSidebarBody>
        <Nav
          aria-label="Main"
          onSelect={() => {
            if (mobile) setSidebarOpen(false);
          }}
        >
          {NAV.map((group) => {
            const items = group.items.map((item) => (
              <NavItem
                key={item.path}
                itemId={item.path}
                to={`#/${ns}${item.path ? `/${item.path}` : ""}`}
                isActive={section === item.path}
              >
                {item.label}
              </NavItem>
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
  );

  return (
    <Page
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
          setSidebarOpen(!mobileView);
        }
      }}
    >
      <Outlet />
    </Page>
  );
}
