// Scroll suave al hacer clic en los links del navbar
document.addEventListener("DOMContentLoaded", () => {
  const navLinks = document.querySelectorAll(".navbar__links a");
  const sections = document.querySelectorAll(".section");
  const toggleBtn = document.querySelector(".navbar__toggle");
  const navContainer = document.querySelector(".navbar__links");

  navLinks.forEach((link) => {
    link.addEventListener("click", (e) => {
      const href = link.getAttribute("href");
      if (href.startsWith("#")) {
        e.preventDefault();
        const target = document.querySelector(href);
        if (target) {
          target.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        // Cerrar menú móvil al hacer clic
        navContainer.classList.remove("active");
      }
    });
  });

  // Menú hamburguesa (mobile)
  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      navContainer.classList.toggle("active");
    });
  }

  // Animación de aparición por scroll
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("visible");
          observer.unobserve(entry.target);
        }
      });
    },
    {
      threshold: 0.2,
    }
  );

  sections.forEach((section) => observer.observe(section));
});
